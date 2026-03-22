/*
  Staging: stg_orders
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        CASE
            WHEN status IN ('completed', 'delivered', 'shipped', 'pending', 'processing') THEN status
            WHEN status IN ('cancelled', 'canceled', 'refunded') THEN 'cancelled'
            ELSE 'unknown'
        END AS status,
        CAST(total_amount AS DECIMAL(12,2)) AS total_amount,
        CAST(COALESCE(shipping_cost, 0) AS DECIMAL(12,2)) AS shipping_cost,
        CAST(total_amount - COALESCE(shipping_cost, 0) AS DECIMAL(12,2)) AS net_amount,
        payment_method,
        EXTRACT(YEAR FROM order_date) AS order_year,
        EXTRACT(MONTH FROM order_date) AS order_month,
        EXTRACT(DOW FROM order_date) IN (0, 6) AS is_weekend

    FROM source
    WHERE total_amount >= 0
)

SELECT * FROM cleaned
