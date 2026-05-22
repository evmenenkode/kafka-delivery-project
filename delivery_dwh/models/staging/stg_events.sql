SELECT
    raw_data:event_id::STRING AS event_id,
    raw_data:event_type::STRING AS event_type,
    raw_data:user_id::STRING AS user_id,
    raw_data:product_id::STRING AS product_id,
    raw_data:amount::NUMERIC(14,2) AS amount,
    raw_data:currency::STRING AS currency,
    raw_data:country::STRING AS country,
    raw_data:session_id::STRING AS session_id,
    raw_data:platform::STRING AS platform,
    raw_data:event_timestamp::TIMESTAMP AS event_ts,
    load_timestamp

FROM {{ source('raw', 'raw_events') }}

WHERE
    raw_data:is_valid::BOOLEAN = TRUE
    AND raw_data:event_id IS NOT NULL
    AND raw_data:user_id IS NOT NULL
    AND raw_data:event_timestamp IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id
    ORDER BY load_timestamp DESC
) = 1