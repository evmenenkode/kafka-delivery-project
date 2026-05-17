--Revenue Anomaly Lift

SELECT *
FROM {{ ref('cdm_daily_kpi') }}
WHERE revenue > (
    SELECT AVG(revenue) * 5
    FROM {{ ref('cdm_daily_kpi') }}
)