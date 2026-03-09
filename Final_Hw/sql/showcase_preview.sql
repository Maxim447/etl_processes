SELECT
    user_id,
    total_sessions,
    total_time_minutes,
    avg_session_minutes,
    unique_pages_visited,
    total_actions,
    most_used_device,
    analysis_date
FROM showcase_user_activity
ORDER BY total_sessions DESC
LIMIT 10;

SELECT
    issue_type,
    status,
    total_tickets,
    avg_resolution_hours,
    max_resolution_hours,
    open_tickets,
    analysis_date
FROM showcase_support_efficiency
ORDER BY total_tickets DESC;