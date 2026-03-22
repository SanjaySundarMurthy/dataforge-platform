/*
  Staging: stg_customers
  ──────────────────────
  Clean and standardize raw customer data.

  Concepts: Source macro, column renaming, type casting,
            COALESCE for defaults, CASE expressions
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        TRIM(first_name) || ' ' || TRIM(last_name) AS full_name,
        LOWER(TRIM(email)) AS email,
        phone,
        CASE
            WHEN LOWER(TRIM(segment)) IN ('premium', 'gold', 'vip') THEN 'premium'
            WHEN LOWER(TRIM(segment)) IN ('standard', 'regular') THEN 'standard'
            ELSE COALESCE(LOWER(TRIM(segment)), 'unknown')
        END AS segment,
        COALESCE(LOWER(TRIM(region)), 'unknown') AS region,
        COALESCE(TRIM(city), 'unknown') AS city,
        COALESCE(TRIM(country), 'unknown') AS country,
        created_at

    FROM source
)

SELECT * FROM cleaned
