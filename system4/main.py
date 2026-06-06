"""System 4 — Drone Tracking Engine.

FastAPI app that:
  - Runs the tracker loop as a background task (via lifespan).
  - Exposes GET /health, GET /tracks, GET /tracks/{id} for System 3.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .db import (
    apply_migrations,
    close_pool,
    count_active_tracks,
    get_pool,
    health_check,
)
from .tracker import run_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

_tracker_task: Optional[asyncio.Task] = None  # type: ignore[type-arg]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _tracker_task
    pool = await get_pool()
    await apply_migrations(pool)
    _tracker_task = asyncio.create_task(run_loop(pool), name="tracker-loop")
    log.info("Tracker loop task started")
    try:
        yield
    finally:
        if _tracker_task and not _tracker_task.done():
            _tracker_task.cancel()
            try:
                await _tracker_task
            except asyncio.CancelledError:
                pass
        await close_pool()
        log.info("System 4 shutdown complete")


app = FastAPI(title="System 4 — Drone Tracking Engine", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class TrackPoint(BaseModel):
    lat: float
    lon: float
    altM: float
    t: int  # ms since epoch


class Track(BaseModel):
    id: int
    status: str
    firstSeen: str
    lastSeen: str
    lastLat: float
    lastLon: float
    lastAltM: float
    pointCount: int
    trail: list[TrackPoint] = []


class TracksResponse(BaseModel):
    tracks: list[Track]
    asOf: str


class HealthResponse(BaseModel):
    status: str
    activeTracks: int
    dbOk: bool


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse)
async def health():
    pool = await get_pool()
    db_ok = await health_check(pool)
    active = await count_active_tracks(pool) if db_ok else 0
    return HealthResponse(status="ok" if db_ok else "degraded", activeTracks=active, dbOk=db_ok)


@app.get("/tracks", response_model=TracksResponse)
async def get_tracks(
    window_m: int = Query(default=5, description="Look-back window in minutes for active tracks"),
    trail_points: int = Query(default=50, description="Max trail points per track"),
):
    """Return recently-active tracks with their trail point history."""
    pool = await get_pool()
    cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=window_m)

    track_rows = await pool.fetch(
        "SELECT id, status, first_seen, last_seen, last_lat, last_lon, last_alt_m, point_count "
        "FROM tracks "
        "WHERE last_seen > $1 "
        "ORDER BY last_seen DESC",
        cutoff,
    )

    if not track_rows:
        return TracksResponse(tracks=[], asOf=datetime.now(tz=timezone.utc).isoformat())

    track_ids = [r["id"] for r in track_rows]

    # Fetch the most-recent trail_points points per track using a window function
    point_rows = await pool.fetch(
        """WITH ranked AS (
             SELECT track_id, lat, lon, alt_m, timestamp,
                    ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY inserted_at DESC) AS rn
             FROM track_points
             WHERE track_id = ANY($1::int[])
           )
           SELECT track_id, lat, lon, alt_m, timestamp
           FROM ranked
           WHERE rn <= $2
           ORDER BY track_id, timestamp ASC""",
        track_ids,
        trail_points,
    )

    # Group points by track_id
    points_by_track: dict[int, list[TrackPoint]] = {tid: [] for tid in track_ids}
    for p in point_rows:
        ts = p["timestamp"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        points_by_track[p["track_id"]].append(
            TrackPoint(lat=p["lat"], lon=p["lon"], altM=p["alt_m"], t=int(ts.timestamp() * 1000))
        )

    tracks = []
    for r in track_rows:
        fs = r["first_seen"]
        ls = r["last_seen"]
        if fs.tzinfo is None:
            fs = fs.replace(tzinfo=timezone.utc)
        if ls.tzinfo is None:
            ls = ls.replace(tzinfo=timezone.utc)
        tracks.append(
            Track(
                id=r["id"],
                status=r["status"],
                firstSeen=fs.isoformat(),
                lastSeen=ls.isoformat(),
                lastLat=r["last_lat"],
                lastLon=r["last_lon"],
                lastAltM=r["last_alt_m"],
                pointCount=r["point_count"],
                trail=points_by_track.get(r["id"], []),
            )
        )

    return TracksResponse(tracks=tracks, asOf=datetime.now(tz=timezone.utc).isoformat())


@app.get("/tracks/{track_id}", response_model=Track)
async def get_track(track_id: int, trail_points: int = Query(default=200)):
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT id, status, first_seen, last_seen, last_lat, last_lon, last_alt_m, point_count "
        "FROM tracks WHERE id = $1",
        track_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Track not found")

    point_rows = await pool.fetch(
        """SELECT lat, lon, alt_m, timestamp
           FROM track_points
           WHERE track_id = $1
           ORDER BY inserted_at ASC
           LIMIT $2""",
        track_id,
        trail_points,
    )

    trail = []
    for p in point_rows:
        ts = p["timestamp"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        trail.append(TrackPoint(lat=p["lat"], lon=p["lon"], altM=p["alt_m"], t=int(ts.timestamp() * 1000)))

    fs = row["first_seen"]
    ls = row["last_seen"]
    if fs.tzinfo is None:
        fs = fs.replace(tzinfo=timezone.utc)
    if ls.tzinfo is None:
        ls = ls.replace(tzinfo=timezone.utc)

    return Track(
        id=row["id"],
        status=row["status"],
        firstSeen=fs.isoformat(),
        lastSeen=ls.isoformat(),
        lastLat=row["last_lat"],
        lastLon=row["last_lon"],
        lastAltM=row["last_alt_m"],
        pointCount=row["point_count"],
        trail=trail,
    )
