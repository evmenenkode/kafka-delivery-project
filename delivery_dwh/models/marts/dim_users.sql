{{ config(
     materialized='table'
)}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id'] )}} AS user_sk,
    user_id,
    first_name,
    last_name,
    email,
    country,
    created_at
FROM {{ ref('stg_users')}}