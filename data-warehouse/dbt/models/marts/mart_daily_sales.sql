/*
  Mart: Daily Sales Summary
  ─────────────────────────
  Aggregated daily sales metrics with running totals.

  Concepts: GROUP BY, Window functions, CTEs, Moving averages
*/

WITH daily_orders AS (
    SELECT
        DATE(o.order_date) AS sale_date,
        o.order_year,
        o.order_month,
        o.is_weekend,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(DISTINCT o.customer_id) AS unique_customers,
        SUM(o.total_amount) AS gross_revenue,
        SUM(o.net_amount) AS net_revenue,
        SUM(o.shipping_cost) AS total_shipping,
        AVG(o.total_amount) AS avg_order_value

    FROM {{ ref('stg_orders') }} o
    WHERE o.status != 'cancelled'
    GROUP BY 1, 2, 3, 4
),

with_running_totals AS (
    SELECT
        *,
        -- Year-to-date revenue
        SUM(gross_revenue) OVER (
            PARTITION BY order_year ORDER BY sale_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS ytd_revenue,

        -- 7-day moving average
        AVG(gross_revenue) OVER (
            ORDER BY sale_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_avg_7d_revenue,

        -- 30-day moving average
        AVG(gross_revenue) OVER (
            ORDER BY sale_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS moving_avg_30d_revenue,

        -- Day-over-day change
        gross_revenue - LAG(gross_revenue) OVER (ORDER BY sale_date) AS dod_change,

        -- Rank within month
        RANK() OVER (
            PARTITION BY order_year, order_month
            ORDER BY gross_revenue DESC
        ) AS month_revenue_rank

    FROM daily_orders
)

SELECT
    *,
    CURRENT_TIMESTAMP AS updated_at

FROM with_running_totals
ORDER BY sale_date
