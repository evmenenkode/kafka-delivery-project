{{ config(
    materialized='table',
    meta={
        "dashboard": "https://datastudio.google.com/reporting/f59aaaaf-5b32-43e7-a57b-f9ac32ce6a3d"
    }
) }}

SELECT
    DATE(f.event_ts) AS date,

    COUNT(DISTINCT f.user_sk) AS active_users,
    COUNT(f.event_id) AS events,
    SUM(COALESCE(f.amount, 0)) AS revenue
FROM {{ ref('fct_events') }} f
GROUP BY 1