CREATE TABLE IF NOT EXISTS volcano_reports (
    report_id TEXT PRIMARY KEY,
    volcano_id TEXT NOT NULL,
    alert_level TEXT NOT NULL,
    eruption_count24h INTEGER NOT NULL,
    ash_column_height_m REAL NOT NULL,
    reported_at DATETIME NOT NULL,
    confidence_level REAL
);
