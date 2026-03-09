TRUNCATE TABLE showcase_support_efficiency;

INSERT INTO showcase_support_efficiency (
    issue_type,
    status,
    total_tickets,
    avg_resolution_hours,
    max_resolution_hours,
    open_tickets,
    analysis_date,
    updated_at
)
SELECT
    issue_type,
    status,
    COUNT(*)                                             AS total_tickets,
    ROUND(
        AVG(
            EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600.0
        )::NUMERIC, 2
    )                                                    AS avg_resolution_hours,
    ROUND(
        MAX(
            EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600.0
        )::NUMERIC, 2
    )                                                    AS max_resolution_hours,
    SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END)    AS open_tickets,
    CURRENT_DATE,
    NOW()
FROM support_tickets
WHERE created_at IS NOT NULL
  AND updated_at IS NOT NULL
  AND updated_at >= created_at
GROUP BY issue_type, status
ORDER BY issue_type, status;