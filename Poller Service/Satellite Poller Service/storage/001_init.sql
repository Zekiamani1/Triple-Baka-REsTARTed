CREATE TABLE IF NOT EXISTS feed_inbox (
    stream_name   TEXT    NOT NULL,
    feed_offset   INTEGER NOT NULL CHECK (feed_offset >= 0),
    event_type    TEXT    NOT NULL,
    raw_payload   BLOB    NOT NULL,
    received_at   TEXT    NOT NULL,
    status        TEXT    NOT NULL DEFAULT 'pending',
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    processed_at  TEXT,
    PRIMARY KEY (stream_name, feed_offset)
);

CREATE INDEX IF NOT EXISTS idx_feed_inbox_pending
    ON feed_inbox(stream_name, status, feed_offset);

CREATE TABLE IF NOT EXISTS checkpoints (
    name        TEXT PRIMARY KEY,
    last_offset INTEGER NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS detections (
    detection_key TEXT PRIMARY KEY,
    satellite     TEXT NOT NULL,
    acq_date      TEXT NOT NULL,
    acq_time      TEXT NOT NULL,
    event_time    TEXT NOT NULL,
    latitude_raw  TEXT NOT NULL,
    longitude_raw TEXT NOT NULL,
    latitude      REAL NOT NULL,
    longitude     REAL NOT NULL,
    confidence    TEXT NOT NULL,
    frp_mw        REAL NOT NULL DEFAULT 0,
    status        TEXT NOT NULL,
    raw_payload   BLOB NOT NULL,
    received_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_detections_event_time
    ON detections(event_time);
CREATE INDEX IF NOT EXISTS idx_detections_status
    ON detections(status);

CREATE TABLE IF NOT EXISTS fire_clusters (
    cluster_id       TEXT PRIMARY KEY,
    source_ref_id    TEXT NOT NULL,
    status           TEXT NOT NULL,
    anchor_key       TEXT NOT NULL,
    latitude         REAL NOT NULL,
    longitude        REAL NOT NULL,
    occurred_at      TEXT NOT NULL,
    detection_count  INTEGER NOT NULL,
    total_frp_mw     REAL NOT NULL,
    severity         TEXT NOT NULL,
    satellites_json  BLOB NOT NULL,
    max_confidence   TEXT NOT NULL,
    detection_keys_json BLOB NOT NULL,
    revision         INTEGER NOT NULL DEFAULT 0,
    updated_at       TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fire_clusters_source_ref_unique
    ON fire_clusters(source_ref_id);

CREATE INDEX IF NOT EXISTS idx_fire_clusters_status
    ON fire_clusters(status);
CREATE INDEX IF NOT EXISTS idx_fire_clusters_source_ref
    ON fire_clusters(source_ref_id);

CREATE TABLE IF NOT EXISTS cluster_members (
    cluster_id   TEXT NOT NULL REFERENCES fire_clusters(cluster_id) ON DELETE CASCADE,
    detection_key TEXT NOT NULL REFERENCES detections(detection_key) ON DELETE CASCADE,
    PRIMARY KEY (cluster_id, detection_key)
);

CREATE INDEX IF NOT EXISTS idx_cluster_members_detection
    ON cluster_members(detection_key);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id      TEXT PRIMARY KEY,
    source_ref_id TEXT NOT NULL,
    revision      INTEGER NOT NULL,
    payload       BLOB NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    TEXT NOT NULL,
    published_at  TEXT
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox_events(status, created_at);
