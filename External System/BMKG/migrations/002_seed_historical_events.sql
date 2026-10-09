WITH RECURSIVE seed(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM seed WHERE n < 20
)
INSERT OR IGNORE INTO seismic_event (
    event_id, magnitude, depth_km, epicenter_lat, epicenter_lon,
    region_name, occurred_at, potential_tsunami
)
SELECT
    printf('bmkg-seed-%02d', n),
    4.5 + (n % 8) * 0.5,
    10.0 + n * 4,
    -8.0 + n * 0.1,
    106.0 + n * 0.1,
    printf('Wilayah Historis %02d', n),
    strftime('%Y-%m-%d %H:%M:%S+00:00', 'now', printf('-%d minutes', 21 - n)),
    CASE WHEN n % 4 = 0 THEN 1 ELSE 0 END
FROM seed;

INSERT OR IGNORE INTO tsunami_warnings (
    warning_id, related_event_id, threat_level, affected_zones, estimated_arrival
)
SELECT
    'bmkg-warning-' || substr(event_id, -2),
    event_id,
    CASE CAST(substr(event_id, -2) AS INTEGER) % 3
        WHEN 0 THEN 'Awas'
        WHEN 1 THEN 'Waspada'
        ELSE 'Siaga'
    END,
    'Zona Historis',
    strftime('%Y-%m-%d %H:%M:%S+00:00', occurred_at, '+45 minutes')
FROM seismic_event
WHERE potential_tsunami = 1;
