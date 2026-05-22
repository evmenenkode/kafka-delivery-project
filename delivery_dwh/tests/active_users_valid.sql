--Active users sanity check

SELECT *
FROM {{ ref('cdm_daily_kpi') }}
WHERE active_users < 0
   OR active_users > (
       SELECT COUNT(*) * 1.2 FROM {{ ref('dim_users') }}
   )