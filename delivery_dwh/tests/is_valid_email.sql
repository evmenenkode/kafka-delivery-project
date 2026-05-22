-- Valid email check (singular test)

SELECT *
FROM {{ ref('stg_users') }}
WHERE email IS NULL
   OR email NOT LIKE '%@%'