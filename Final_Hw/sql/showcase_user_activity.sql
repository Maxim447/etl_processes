TRUNCATE TABLE showcase_user_activity;


INSERT INTO showcase_user_activity (
    user_id,
    total_sessions,
    total_time_minutes,
    avg_session_minutes,
    unique_pages_visited,
    total_actions,
    most_used_device,
    analysis_date,
    updated_at
)
WITH session_stats AS (
    SELECT
        user_id,
        COUNT(*)                                        AS total_sessions,
        SUM(
            EXTRACT(EPOCH FROM (end_time - start_time)) / 60.0
        )                                               AS total_time_minutes,
        AVG(
            EXTRACT(EPOCH FROM (end_time - start_time)) / 60.0
        )                                               AS avg_session_minutes,
        COUNT(DISTINCT unnested_page)                   AS unique_pages_visited,
        SUM(CARDINALITY(actions))                       AS total_actions
    FROM user_sessions
    CROSS JOIN LATERAL UNNEST(pages_visited) AS unnested_page
    WHERE start_time IS NOT NULL
      AND end_time   IS NOT NULL
      AND end_time   > start_time
    GROUP BY user_id
),
device_mode AS (
    SELECT DISTINCT ON (user_id)
        user_id,
        device AS most_used_device
    FROM user_sessions
    GROUP BY user_id, device
    ORDER BY user_id, COUNT(*) DESC
)
SELECT
    ss.user_id,
    ss.total_sessions,
    ROUND(ss.total_time_minutes::NUMERIC,  2),
    ROUND(ss.avg_session_minutes::NUMERIC, 2),
    ss.unique_pages_visited,
    ss.total_actions,
    dm.most_used_device,
    CURRENT_DATE,
    NOW()
FROM session_stats  ss
LEFT JOIN device_mode dm USING (user_id);