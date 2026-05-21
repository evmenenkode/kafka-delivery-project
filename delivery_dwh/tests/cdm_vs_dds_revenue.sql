--Revenue consistency (CDM vs DDS)

SELECT *
FROM (
    SELECT
        d.date,
        d.revenue AS cdm_rev,
        f.rev AS dds_rev
    FROM {{ ref('cdm_daily_kpi') }} d
    LEFT JOIN (
        SELECT DATE(event_ts) AS date, SUM(amount) AS rev
        FROM {{ ref('fct_events') }}
        GROUP BY 1
    ) f
    ON d.date = f.date
)
WHERE ABS(cdm_rev - dds_rev) > 0.01