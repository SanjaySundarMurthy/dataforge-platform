/*
  Staging: stg_order_items
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'order_items') }}
),

cleaned AS (
    SELECT
        item_id,
        order_id,
        product_id,
        quantity,
        CAST(unit_price AS DECIMAL(12,2)) AS unit_price,
        CAST(COALESCE(discount, 0) AS DECIMAL(5,4)) AS discount,
        ROUND(quantity * unit_price * (1 - COALESCE(discount, 0)), 2) AS line_total

    FROM source
    WHERE quantity > 0 AND unit_price > 0
)

SELECT * FROM cleaned
