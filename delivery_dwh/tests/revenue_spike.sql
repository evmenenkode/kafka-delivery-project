--Revenue Anomaly Lift

SELECT *
FROM {{ ref('cdm_daily_kpi') }}
WHERE revenue > (
    SELECT AVG(revenue) * 3
    FROM {{ ref('cdm_daily_kpi') }}
    WHERE date >= DATEADD(day, -30, CURRENT_DATE)
)