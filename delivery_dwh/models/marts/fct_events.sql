{{ config(
    materialized='incremental',
    unique_key='event_id'
)}}

SELECT
    e.event_id,
    e.event_type,
    u.user_sk,
    p.product_sk,
    e.amount,
    e.currency,
    e.country,
    e.platform,
    e.session_id,
    e.event_ts
FROM {{ ref('stg_events')}} e
LEFT JOIN {{ ref('dim_users')}} u 
    ON e.user_id=u.user_id
LEFT JOIN {{ ref('dim_products')}} p
    ON e.product_id=p.product_id

{% if is_incremental() %}
WHERE e.event_ts > (SELECT MAX(event_ts) FROM {{ this}})
{% endif %}