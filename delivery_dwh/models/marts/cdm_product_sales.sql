{{ config(
    materialized='table',
    meta={
        "dashboard": "https://datastudio.google.com/reporting/f59aaaaf-5b32-43e7-a57b-f9ac32ce6a3d"
    }
) }}

SELECT
    p.product_id,
    p.category,

    COUNT(f.event_id) AS purchases,
    SUM(COALESCE(f.amount, 0)) AS revenue,
    AVG(f.amount) AS avg_order_value
FROM {{ ref('dim_products') }} p
LEFT JOIN {{ ref('fct_events') }} f
    ON p.product_sk = f.product_sk
GROUP BY 1,2