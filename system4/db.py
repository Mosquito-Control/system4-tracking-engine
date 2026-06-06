"""asyncpg-based DB helpers for reading positions and writing tracks."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import asyncpg

log = logging.getLogger(__name__)

_pool: Optional[asyncpg.Pool] = None
_MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"


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


async def apply_migrations(pool: asyncpg.Pool) -> None:
    """Run every .sql file in migrations/ in lexical order.

    Each file is wrapped in a transaction; statements use IF NOT EXISTS so
    re-runs are no-ops. Crashes loudly if the migrations dir is missing,
    rather than letting the app boot against a schema-less DB.
    """
    if not _MIGRATIONS_DIR.is_dir():
        raise RuntimeError(f"migrations dir not found: {_MIGRATIONS_DIR}")
    files = sorted(_MIGRATIONS_DIR.glob("*.sql"))
    if not files:
        log.warning("No migrations found in %s", _MIGRATIONS_DIR)
        return
    async with pool.acquire() as conn:
        for f in files:
            sql = f.read_text()
            async with conn.transaction():
                await conn.execute(sql)
            log.info("Applied migration: %s", f.name)


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


async def delete_old_lost_tracks(pool: asyncpg.Pool, purge_after_s: float) -> int:
    """Hard-delete tracks that have been 'lost' beyond purge_after_s.

    Cascades to track_points via the FK (no explicit cascade — done manually
    to avoid an existing-schema migration). Returns rows deleted.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            ids = await conn.fetch(
                """SELECT id FROM tracks
                   WHERE status = 'lost'
                     AND last_seen < NOW() - ($1 || ' seconds')::INTERVAL""",
                str(purge_after_s),
            )
            if not ids:
                return 0
            id_list = [r["id"] for r in ids]
            await conn.execute(
                "DELETE FROM track_points WHERE track_id = ANY($1::int[])", id_list
            )
            await conn.execute("DELETE FROM tracks WHERE id = ANY($1::int[])", id_list)
            return len(id_list)


async def health_check(pool: asyncpg.Pool) -> bool:
    try:
        await pool.fetchval("SELECT 1")
        return True
    except Exception:
        return False


async def count_active_tracks(pool: asyncpg.Pool) -> int:
    return await pool.fetchval("SELECT COUNT(*) FROM tracks WHERE status = 'active'")  # type: ignore[return-value]
