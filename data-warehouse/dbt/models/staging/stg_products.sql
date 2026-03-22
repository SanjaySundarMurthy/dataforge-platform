/*
  Staging: stg_products
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
),

cleaned AS (
    SELECT
        product_id,
        TRIM(product_name) AS product_name,
        LOWER(TRIM(category)) AS category,
        LOWER(TRIM(subcategory)) AS subcategory,
        LOWER(TRIM(brand)) AS brand,
        CAST(price AS DECIMAL(12,2)) AS price,
        CAST(cost AS DECIMAL(12,2)) AS cost,
        ROUND((price - cost) / NULLIF(price, 0) * 100, 2) AS profit_margin,
        CASE
            WHEN price < 25 THEN 'budget'
            WHEN price < 100 THEN 'mid-range'
            WHEN price < 500 THEN 'premium'
            ELSE 'luxury'
        END AS price_tier,
        weight_kg,
        created_at

    FROM source
    WHERE price > 0 AND cost > 0
)

SELECT * FROM cleaned
