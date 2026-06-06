"""asyncpg-based DB helpers for reading positions and writing tracks."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import asyncpg

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=os.environ["DATABASE_URL"],
            min_size=2,
            max_size=5,
            command_timeout=10,
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


@dataclass
class RawPosition:
    id: int
    timestamp: datetime
    lat: float
    lon: float
    alt_m: float
    cam_pair: str
    inserted_at: datetime


@dataclass
class TrackRow:
    id: int
    first_seen: datetime
    last_seen: datetime
    last_lat: float
    last_lon: float
    last_alt_m: float
    point_count: int
    status: str


async def fetch_new_positions(pool: asyncpg.Pool, since: Optional[datetime]) -> list[RawPosition]:
    if since is None:
        # On cold start fetch last 60 seconds to bootstrap any tracks
        rows = await pool.fetch(
            """SELECT id, timestamp, lat, lon, alt_m, cam_pair, inserted_at
               FROM positions
               WHERE inserted_at > NOW() - INTERVAL '60 seconds'
               ORDER BY inserted_at ASC""",
        )
    else:
        rows = await pool.fetch(
            """SELECT id, timestamp, lat, lon, alt_m, cam_pair, inserted_at
               FROM positions
               WHERE inserted_at > $1
               ORDER BY inserted_at ASC""",
            since,
        )
    return [
        RawPosition(
            id=r["id"],
            timestamp=r["timestamp"],
            lat=r["lat"],
            lon=r["lon"],
            alt_m=r["alt_m"],
            cam_pair=r["cam_pair"],
            inserted_at=r["inserted_at"],
        )
        for r in rows
    ]


async def fetch_active_tracks(pool: asyncpg.Pool) -> list[TrackRow]:
    rows = await pool.fetch(
        "SELECT id, first_seen, last_seen, last_lat, last_lon, last_alt_m, point_count, status "
        "FROM tracks WHERE status = 'active'"
    )
    return [
        TrackRow(
            id=r["id"],
            first_seen=r["first_seen"],
            last_seen=r["last_seen"],
            last_lat=r["last_lat"],
            last_lon=r["last_lon"],
            last_alt_m=r["last_alt_m"],
            point_count=r["point_count"],
            status=r["status"],
        )
        for r in rows
    ]


async def insert_track(pool: asyncpg.Pool, pos: RawPosition) -> int:
    row = await pool.fetchrow(
        """INSERT INTO tracks (first_seen, last_seen, last_lat, last_lon, last_alt_m, point_count, status)
           VALUES ($1, $2, $3, $4, $5, 1, 'active')
           RETURNING id""",
        pos.timestamp,
        pos.timestamp,
        pos.lat,
        pos.lon,
        pos.alt_m,
    )
    return row["id"]  # type: ignore[index]


async def update_track(pool: asyncpg.Pool, track_id: int, pos: RawPosition) -> None:
    await pool.execute(
        """UPDATE tracks
           SET last_seen   = $1,
               last_lat    = $2,
               last_lon    = $3,
               last_alt_m  = $4,
               point_count = point_count + 1,
               status      = 'active'
           WHERE id = $5""",
        pos.timestamp,
        pos.lat,
        pos.lon,
        pos.alt_m,
        track_id,
    )


async def insert_track_point(pool: asyncpg.Pool, track_id: int, pos: RawPosition) -> None:
    await pool.execute(
        """INSERT INTO track_points (track_id, position_id, timestamp, lat, lon, alt_m)
           VALUES ($1, $2, $3, $4, $5, $6)""",
        track_id,
        pos.id,
        pos.timestamp,
        pos.lat,
        pos.lon,
        pos.alt_m,
    )


async def mark_lost_tracks(pool: asyncpg.Pool, lost_after_s: float) -> None:
    await pool.execute(
        """UPDATE tracks
           SET status = 'lost'
           WHERE status = 'active'
             AND last_seen < NOW() - ($1 || ' seconds')::INTERVAL""",
        str(lost_after_s),
    )


async def health_check(pool: asyncpg.Pool) -> bool:
    try:
        await pool.fetchval("SELECT 1")
        return True
    except Exception:
        return False


async def count_active_tracks(pool: asyncpg.Pool) -> int:
    return await pool.fetchval("SELECT COUNT(*) FROM tracks WHERE status = 'active'")  # type: ignore[return-value]
