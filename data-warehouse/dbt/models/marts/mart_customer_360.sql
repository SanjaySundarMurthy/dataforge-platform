/*
  Mart: Customer 360
  ──────────────────
  Comprehensive customer profile combining orders, reviews,
  and behavioral data with RFM segmentation.

  Concepts: Multi-table JOINs, CASE expressions, RFM scoring,
            COALESCE for defaults, subqueries
*/

WITH order_metrics AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_amount) AS total_spend,
        AVG(total_amount) AS avg_order_value,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        EXTRACT(DAY FROM CURRENT_TIMESTAMP - MAX(order_date)) AS days_since_last_order

    FROM {{ ref('stg_orders') }}
    WHERE status != 'cancelled'
    GROUP BY customer_id
),

item_metrics AS (
    SELECT
        o.customer_id,
        SUM(oi.quantity) AS total_items_purchased,
        COUNT(DISTINCT oi.product_id) AS unique_products

    FROM {{ ref('stg_order_items') }} oi
    INNER JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
    WHERE o.status != 'cancelled'
    GROUP BY o.customer_id
),

combined AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.email,
        c.segment,
        c.region,
        c.city,
        c.country,
        c.created_at AS customer_since,

        -- Order metrics
        COALESCE(om.total_orders, 0) AS total_orders,
        COALESCE(om.total_spend, 0) AS total_spend,
        COALESCE(om.avg_order_value, 0) AS avg_order_value,
        om.first_order_date,
        om.last_order_date,
        COALESCE(om.days_since_last_order, 9999) AS days_since_last_order,

        -- Item metrics
        COALESCE(im.total_items_purchased, 0) AS total_items_purchased,
        COALESCE(im.unique_products, 0) AS unique_products,

        -- RFM Scoring
        CASE
            WHEN om.days_since_last_order <= 30 THEN 5
            WHEN om.days_since_last_order <= 90 THEN 4
            WHEN om.days_since_last_order <= 180 THEN 3
            WHEN om.days_since_last_order <= 365 THEN 2
            ELSE 1
        END AS rfm_recency,

        CASE
            WHEN COALESCE(om.total_orders, 0) >= 20 THEN 5
            WHEN COALESCE(om.total_orders, 0) >= 10 THEN 4
            WHEN COALESCE(om.total_orders, 0) >= 5 THEN 3
            WHEN COALESCE(om.total_orders, 0) >= 2 THEN 2
            ELSE 1
        END AS rfm_frequency,

        CASE
            WHEN COALESCE(om.total_spend, 0) >= 5000 THEN 5
            WHEN COALESCE(om.total_spend, 0) >= 2000 THEN 4
            WHEN COALESCE(om.total_spend, 0) >= 500 THEN 3
            WHEN COALESCE(om.total_spend, 0) >= 100 THEN 2
            ELSE 1
        END AS rfm_monetary

    FROM {{ ref('stg_customers') }} c
    LEFT JOIN order_metrics om ON c.customer_id = om.customer_id
    LEFT JOIN item_metrics im ON c.customer_id = im.customer_id
)

SELECT
    *,
    -- Customer tier based on RFM
    CASE
        WHEN rfm_recency >= 4 AND rfm_frequency >= 4 AND rfm_monetary >= 4 THEN 'champion'
        WHEN rfm_recency >= 3 AND rfm_frequency >= 3 THEN 'loyal'
        WHEN rfm_recency >= 4 THEN 'new_active'
        WHEN rfm_recency <= 2 THEN 'at_risk'
        ELSE 'regular'
    END AS customer_tier,

    -- Estimated lifetime value
    ROUND(avg_order_value * total_orders * 1.2, 2) AS estimated_ltv,

    CURRENT_TIMESTAMP AS updated_at

FROM combined
