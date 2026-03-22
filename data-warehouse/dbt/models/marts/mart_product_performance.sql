/*
  Mart: Product Performance
  ─────────────────────────
  Product KPIs including revenue, units, ratings, and rankings.

  Concepts: Multiple JOINs, RANK window function, CASE
*/

WITH sales AS (
    SELECT
        oi.product_id,
        SUM(oi.line_total) AS total_revenue,
        SUM(oi.quantity) AS units_sold,
        COUNT(DISTINCT oi.order_id) AS order_count,
        AVG(oi.unit_price) AS avg_selling_price

    FROM {{ ref('stg_order_items') }} oi
    INNER JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
    WHERE o.status != 'cancelled'
    GROUP BY oi.product_id
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.price AS list_price,
    p.cost,
    p.profit_margin,
    p.price_tier,

    COALESCE(s.total_revenue, 0) AS total_revenue,
    COALESCE(s.units_sold, 0) AS units_sold,
    COALESCE(s.order_count, 0) AS order_count,
    COALESCE(s.avg_selling_price, p.price) AS avg_selling_price,

    -- Revenue rank within category
    RANK() OVER (
        PARTITION BY p.category
        ORDER BY COALESCE(s.total_revenue, 0) DESC
    ) AS category_revenue_rank,

    -- Overall revenue rank
    RANK() OVER (
        ORDER BY COALESCE(s.total_revenue, 0) DESC
    ) AS overall_revenue_rank,

    CURRENT_TIMESTAMP AS updated_at

FROM {{ ref('stg_products') }} p
LEFT JOIN sales s ON p.product_id = s.product_id
