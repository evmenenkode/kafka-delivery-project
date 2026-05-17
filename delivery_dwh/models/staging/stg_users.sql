SELECT
    raw_data:user_id::STRING AS user_id,
    raw_data:first_name::STRING AS first_name,
    raw_data:last_name::STRING AS last_name,
    raw_data:email::STRING AS email,
    raw_data:country::STRING AS country,
    raw_data:created_at::TIMESTAMP AS created_at,
    load_timestamp
FROM {{ source('raw', 'raw_users') }}