-- System 4 schema migration
-- Run with droneadmin credentials against drone-detection-pg / dronedetection

CREATE TABLE IF NOT EXISTS tracks (
  id          SERIAL PRIMARY KEY,
  first_seen  TIMESTAMPTZ NOT NULL,
  last_seen   TIMESTAMPTZ NOT NULL,
  last_lat    DOUBLE PRECISION NOT NULL,
  last_lon    DOUBLE PRECISION NOT NULL,
  last_alt_m  DOUBLE PRECISION NOT NULL,
  point_count INTEGER NOT NULL DEFAULT 1,
  status      TEXT NOT NULL DEFAULT 'active'  -- 'active' | 'lost'
);

CREATE TABLE IF NOT EXISTS track_points (
  id          SERIAL PRIMARY KEY,
  track_id    INTEGER NOT NULL REFERENCES tracks(id),
  position_id INTEGER NOT NULL REFERENCES positions(id),
  timestamp   TIMESTAMPTZ NOT NULL,
  lat         DOUBLE PRECISION NOT NULL,
  lon         DOUBLE PRECISION NOT NULL,
  alt_m       DOUBLE PRECISION NOT NULL,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS track_points_track_id_idx    ON track_points(track_id);
CREATE INDEX IF NOT EXISTS track_points_inserted_at_idx ON track_points(inserted_at);
CREATE INDEX IF NOT EXISTS tracks_last_seen_idx         ON tracks(last_seen);
CREATE INDEX IF NOT EXISTS tracks_status_idx            ON tracks(status);

-- System 3 read-only role needs SELECT on the new tables
GRANT SELECT ON tracks       TO system3_reader;
GRANT SELECT ON track_points TO system3_reader;
