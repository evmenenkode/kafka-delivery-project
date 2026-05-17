--Active users sanity check

SELECT *
FROM {{ ref('cdm_daily_kpi') }}
WHERE active_users >
(
    SELECT COUNT(*) FROM {{ ref('dim_users') }}
)