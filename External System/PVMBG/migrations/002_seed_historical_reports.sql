WITH RECURSIVE seed(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM seed WHERE n < 20
)
INSERT INTO volcano_reports (
    report_id, volcano_id, alert_level, eruption_count_24h,
    ash_column_height_m, reported_at
)
SELECT
    printf('pvmbg-seed-%02d', n),
    CASE n % 5
        WHEN 0 THEN 'Merapi'
        WHEN 1 THEN 'Semeru'
        WHEN 2 THEN 'Kelud'
        WHEN 3 THEN 'Papandayan'
        ELSE 'Bromo'
    END,
    CASE n % 4
        WHEN 0 THEN 'Normal'
        WHEN 1 THEN 'Waspada'
        WHEN 2 THEN 'Siaga'
        ELSE 'Awas'
    END,
    n * 2,
    n * 125.0,
    datetime('now', printf('-%d minutes', 21 - n))
FROM seed;
