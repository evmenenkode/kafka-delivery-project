{{ config(
    materialized='table',
    meta={
        "dashboard": "https://datastudio.google.com/reporting/f59aaaaf-5b32-43e7-a57b-f9ac32ce6a3d"
    }
) }}

SELECT
    u.user_id,
    u.country,

    COUNT(f.event_id) AS total_events,
    COUNT(DISTINCT DATE(f.event_ts)) AS active_days,
    SUM(COALESCE(f.amount, 0)) AS revenue,
    MAX(f.event_ts) AS last_activity
FROM {{ ref('dim_users') }} u
LEFT JOIN {{ ref('fct_events') }} f
    ON u.user_sk = f.user_sk
GROUP BY 1,2