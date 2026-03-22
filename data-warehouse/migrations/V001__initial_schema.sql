-- ============================================================
-- DataForge Data Warehouse - Schema Migrations
-- V001: Initial schema creation
-- ============================================================

-- ── Raw Schema (for staging/landing) ────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;

-- ── Staging Schema ──────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS staging;

-- ── Warehouse Schema (Star Schema) ─────────────────────────
CREATE SCHEMA IF NOT EXISTS warehouse;

-- ── Analytics Schema (Aggregations & Views) ─────────────────
CREATE SCHEMA IF NOT EXISTS analytics;

-- ════════════════════════════════════════════════════════════
-- Dimension Tables
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key        INT PRIMARY KEY,
    full_date       DATE NOT NULL,
    year            INT NOT NULL,
    quarter         INT NOT NULL,
    month           INT NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    week            INT NOT NULL,
    day_of_month    INT NOT NULL,
    day_of_week     INT NOT NULL,
    day_name        VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_holiday      BOOLEAN DEFAULT FALSE,
    fiscal_year     INT,
    fiscal_quarter  INT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_key    SERIAL PRIMARY KEY,
    customer_id     VARCHAR(50) NOT NULL UNIQUE,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    full_name       VARCHAR(200),
    email           VARCHAR(255),
    phone           VARCHAR(50),
    segment         VARCHAR(50),
    region          VARCHAR(100),
    city            VARCHAR(100),
    country         VARCHAR(100),
    customer_tier   VARCHAR(50),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active       BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key     SERIAL PRIMARY KEY,
    product_id      VARCHAR(50) NOT NULL UNIQUE,
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    subcategory     VARCHAR(100),
    brand           VARCHAR(100),
    price           DECIMAL(12, 2),
    cost            DECIMAL(12, 2),
    profit_margin   DECIMAL(5, 2),
    price_tier      VARCHAR(50),
    weight_kg       DECIMAL(8, 2),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active       BOOLEAN DEFAULT TRUE
);

-- ════════════════════════════════════════════════════════════
-- Fact Tables
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_key           SERIAL PRIMARY KEY,
    order_id            VARCHAR(50) NOT NULL UNIQUE,
    customer_key        INT REFERENCES warehouse.dim_customer(customer_key),
    order_date_key      INT REFERENCES warehouse.dim_date(date_key),
    status              VARCHAR(50),
    total_amount        DECIMAL(12, 2),
    shipping_cost       DECIMAL(12, 2),
    net_amount          DECIMAL(12, 2),
    payment_method      VARCHAR(50),
    order_year          INT,
    order_month         INT,
    is_weekend          BOOLEAN,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.fact_order_items (
    item_key        SERIAL PRIMARY KEY,
    item_id         VARCHAR(50) NOT NULL UNIQUE,
    order_key       INT REFERENCES warehouse.fact_orders(order_key),
    product_key     INT REFERENCES warehouse.dim_product(product_key),
    quantity        INT,
    unit_price      DECIMAL(12, 2),
    discount        DECIMAL(5, 4),
    line_total      DECIMAL(12, 2),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ════════════════════════════════════════════════════════════
-- Analytics Tables (Pre-computed Aggregations)
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS analytics.daily_sales (
    sale_date           DATE NOT NULL,
    total_orders        INT,
    unique_customers    INT,
    gross_revenue       DECIMAL(14, 2),
    net_revenue         DECIMAL(14, 2),
    avg_order_value     DECIMAL(12, 2),
    ytd_revenue         DECIMAL(16, 2),
    moving_avg_7d       DECIMAL(12, 2),
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sale_date)
);

CREATE TABLE IF NOT EXISTS analytics.customer_360 (
    customer_id         VARCHAR(50) PRIMARY KEY,
    full_name           VARCHAR(200),
    segment             VARCHAR(50),
    region              VARCHAR(100),
    total_orders        INT,
    total_spend         DECIMAL(14, 2),
    avg_order_value     DECIMAL(12, 2),
    days_since_last     INT,
    total_reviews       INT,
    avg_rating_given    DECIMAL(3, 2),
    customer_tier       VARCHAR(50),
    rfm_recency         INT,
    rfm_frequency       INT,
    rfm_monetary        INT,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.product_performance (
    product_id          VARCHAR(50) PRIMARY KEY,
    product_name        VARCHAR(255),
    category            VARCHAR(100),
    brand               VARCHAR(100),
    total_revenue       DECIMAL(14, 2),
    units_sold          INT,
    order_count         INT,
    avg_rating          DECIMAL(3, 2),
    review_count        INT,
    category_rank       INT,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ════════════════════════════════════════════════════════════
-- Pipeline Logging
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS raw.pipeline_log (
    log_id          SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(255),
    table_name      VARCHAR(255),
    rows_processed  BIGINT,
    status          VARCHAR(50),
    execution_time  INT,
    error_message   TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ════════════════════════════════════════════════════════════
-- Indexes
-- ════════════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON warehouse.fact_orders(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON warehouse.fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_items_order ON warehouse.fact_order_items(order_key);
CREATE INDEX IF NOT EXISTS idx_fact_items_product ON warehouse.fact_order_items(product_key);
CREATE INDEX IF NOT EXISTS idx_daily_sales_date ON analytics.daily_sales(sale_date);
