SELECT
    raw_data:product_id::STRING AS product_id,
    raw_data:product_name::STRING AS product_name,
    raw_data:category::STRING AS category,
    raw_data:price::NUMERIC(14,2) AS price,
    raw_data:created_at::TIMESTAMP AS created_at
FROM {{ source('raw', 'raw_products') }}