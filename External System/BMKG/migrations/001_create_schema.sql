CREATE TABLE IF NOT EXISTS seismic_event (
    event_id VARCHAR(50) PRIMARY KEY,
    magnitude DOUBLE PRECISION NOT NULL,
    depth_km DOUBLE PRECISION NOT NULL,
    epicenter_lat DOUBLE PRECISION NOT NULL,
    epicenter_lon DOUBLE PRECISION NOT NULL,
    region_name VARCHAR(100) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    potential_tsunami BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS tsunami_warnings (
    warning_id VARCHAR(50) PRIMARY KEY,
    related_event_id VARCHAR(50) NOT NULL,
    threat_level VARCHAR(10) NOT NULL,
    affected_zones TEXT NOT NULL,
    estimated_arrival TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (related_event_id) REFERENCES seismic_event(event_id)
);
