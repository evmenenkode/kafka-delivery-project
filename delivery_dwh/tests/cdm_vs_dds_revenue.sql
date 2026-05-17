--Revenue consistency (CDM vs DDS)

SELECT
    (SELECT SUM(revenue) FROM {{ ref('cdm_daily_kpi') }}) AS cdm_rev,
    (SELECT SUM(amount) FROM {{ ref('fct_events') }}) AS dds_rev
WHERE ABS(
    (SELECT SUM(revenue) FROM {{ ref('cdm_daily_kpi') }})
    -
    (SELECT SUM(amount) FROM {{ ref('fct_events') }})
) > 0.01