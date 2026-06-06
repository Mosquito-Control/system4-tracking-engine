"""Greedy nearest-neighbour drone tracker.

Runs as a background asyncio task. On each tick:
  1. Fetch positions inserted since the last cursor.
  2. Load all currently active tracks.
  3. For each new position, find the nearest active track within the spatial
     and temporal association windows; link it, or start a new track.
  4. Mark tracks idle for longer than LOST_AFTER_S as 'lost'.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import asyncpg

from .db import (
    RawPosition,
    TrackRow,
    count_active_tracks,
    delete_old_lost_tracks,
    fetch_active_tracks,
    fetch_new_positions,
    insert_track,
    insert_track_point,
    mark_lost_tracks,
    update_track,
)
from .geo import haversine_m

log = logging.getLogger(__name__)

ASSOC_DIST_M = float(os.getenv("ASSOC_DIST_M", "50"))
ASSOC_TIME_S = float(os.getenv("ASSOC_TIME_S", "3.0"))
LOST_AFTER_S = float(os.getenv("LOST_AFTER_S", "10.0"))
LOOP_INTERVAL_S = float(os.getenv("LOOP_INTERVAL_S", "1.0"))
# Hard-delete tracks that have been 'lost' for longer than this. Prevents
# unbounded growth of the tracks/track_points tables during continuous run.
PURGE_AFTER_S = float(os.getenv("PURGE_AFTER_S", "3600"))  # 1 hour


def _seconds_between(a: datetime, b: datetime) -> float:
    """Absolute elapsed seconds, timezone-aware safe."""
    if a.tzinfo is None:
        a = a.replace(tzinfo=timezone.utc)
    if b.tzinfo is None:
        b = b.replace(tzinfo=timezone.utc)
    return abs((b - a).total_seconds())


def _find_best_track(pos: RawPosition, active: list[TrackRow]) -> Optional[TrackRow]:
    best: Optional[TrackRow] = None
    best_dist = float("inf")
    for track in active:
        if _seconds_between(track.last_seen, pos.timestamp) > ASSOC_TIME_S:
            continue
        dist = haversine_m(track.last_lat, track.last_lon, pos.lat, pos.lon)
        if dist <= ASSOC_DIST_M and dist < best_dist:
            best_dist = dist
            best = track
    return best


async def run_loop(pool: asyncpg.Pool) -> None:
    cursor: Optional[datetime] = None
    # In-memory mirror of active tracks so we don't re-query after every point
    # within the same tick. Refreshed from DB at the start of each tick.
    active_tracks: list[TrackRow] = []

    log.info("Tracker loop started (dist=%.0fm  time=%.1fs  lost=%.1fs  purge=%.0fs)",
             ASSOC_DIST_M, ASSOC_TIME_S, LOST_AFTER_S, PURGE_AFTER_S)
    tick = 0

    while True:
        try:
            new_positions = await fetch_new_positions(pool, cursor)

            if new_positions:
                # Refresh active track list once per tick
                active_tracks = await fetch_active_tracks(pool)

                for pos in new_positions:
                    match = _find_best_track(pos, active_tracks)

                    if match:
                        await update_track(pool, match.id, pos)
                        # FK on track_points.position_id can fire if S2's
                        # commit hasn't propagated yet — skip the point but
                        # keep the track update so we don't lose the tick.
                        try:
                            await insert_track_point(pool, match.id, pos)
                        except asyncpg.ForeignKeyViolationError:
                            log.warning("track_point FK miss (track=%d, pos=%d) — skipped",
                                        match.id, pos.id)
                        # Update in-memory state so later positions in this
                        # tick can link to the same track with updated coords.
                        match.last_lat = pos.lat
                        match.last_lon = pos.lon
                        match.last_alt_m = pos.alt_m
                        match.last_seen = pos.timestamp
                        match.point_count += 1
                    else:
                        new_id = await insert_track(pool, pos)
                        try:
                            await insert_track_point(pool, new_id, pos)
                        except asyncpg.ForeignKeyViolationError:
                            log.warning("track_point FK miss on new track (pos=%d) — skipped",
                                        pos.id)
                        active_tracks.append(
                            TrackRow(
                                id=new_id,
                                first_seen=pos.timestamp,
                                last_seen=pos.timestamp,
                                last_lat=pos.lat,
                                last_lon=pos.lon,
                                last_alt_m=pos.alt_m,
                                point_count=1,
                                status="active",
                            )
                        )

                cursor = new_positions[-1].inserted_at
                n = await count_active_tracks(pool)
                log.debug("Processed %d positions → %d active tracks", len(new_positions), n)

            await mark_lost_tracks(pool, LOST_AFTER_S)
            # Hard-delete old lost tracks every ~60 ticks (~1 min at default).
            tick += 1
            if tick % 60 == 0:
                await delete_old_lost_tracks(pool, PURGE_AFTER_S)

        except asyncio.CancelledError:
            log.info("Tracker loop cancelled")
            return
        except Exception:
            log.exception("Tracker loop error — retrying next tick")

        await asyncio.sleep(LOOP_INTERVAL_S)
