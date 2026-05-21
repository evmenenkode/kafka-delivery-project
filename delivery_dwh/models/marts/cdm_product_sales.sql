{{ config(
    materialized='table'
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