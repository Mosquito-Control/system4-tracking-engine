-- Window function in GET /tracks partitions by track_id and sorts by inserted_at.
-- The pair index avoids per-track sort on retrieval as track_points grows.

CREATE INDEX IF NOT EXISTS track_points_track_inserted_idx
  ON track_points(track_id, inserted_at DESC);

-- delete_old_lost_tracks filters on (status='lost', last_seen < ...). Without
-- this index the purge job seq-scans the whole tracks table.
CREATE INDEX IF NOT EXISTS tracks_status_last_seen_idx
  ON tracks(status, last_seen);
