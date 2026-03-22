# 🏢 Data Warehouse — Star Schema & dbt

> Star schema design with dbt transformations for analytics-ready data serving.

---

## 🏛️ Schema Design

```mermaid
erDiagram
    dim_date ||--o{ fact_orders : "order_date"
    dim_customer ||--o{ fact_orders : "customer_id"
    dim_product ||--o{ fact_order_items : "product_id"
    fact_orders ||--o{ fact_order_items : "order_id"

    dim_date {
        date date_key PK
        int day_of_week
        string month_name
        int quarter
        bool is_weekend
    }

    dim_customer {
        string customer_id PK
        string full_name
        string email
        string segment
        string city
        string country
    }

    dim_product {
        string product_id PK
        string name
        string category
        string brand
        decimal price
    }

    fact_orders {
        string order_id PK
        string customer_id FK
        date order_date FK
        decimal total_amount
        string status
        string payment_method
    }

    fact_order_items {
        string item_id PK
        string order_id FK
        string product_id FK
        int quantity
        decimal unit_price
        decimal discount
    }
```

---

## 📁 Structure

```
data-warehouse/
├── migrations/
│   └── V001__initial_schema.sql     # Star schema DDL
└── dbt/
    ├── dbt_project.yml              # dbt configuration
    ├── packages.yml                 # Dependencies (dbt_utils)
    ├── profiles.yml                 # Connection profiles
    ├── models/
    │   ├── staging/                 # 1:1 source cleaning
    │   │   ├── stg_customers.sql
    │   │   ├── stg_orders.sql
    │   │   ├── stg_order_items.sql
    │   │   └── schema.yml          # Column tests
    │   └── marts/                   # Business aggregations
    │       ├── mart_daily_sales.sql
    │       ├── mart_customer_360.sql
    │       └── schema.yml
    ├── macros/
    │   └── generate_date_spine.sql  # Reusable date helper
    └── tests/                       # Custom data tests
```

---

## 🔄 dbt Model Flow

```mermaid
graph LR
    subgraph "Sources"
        RAW_CUST[raw_customers]
        RAW_ORD[raw_orders]
        RAW_ITEMS[raw_order_items]
    end

    subgraph "Staging (1:1 clean)"
        STG_CUST[stg_customers]
        STG_ORD[stg_orders]
        STG_ITEMS[stg_order_items]
    end

    subgraph "Marts (business logic)"
        MART_SALES[mart_daily_sales]
        MART_360[mart_customer_360]
    end

    RAW_CUST --> STG_CUST
    RAW_ORD --> STG_ORD
    RAW_ITEMS --> STG_ITEMS
    STG_CUST & STG_ORD --> MART_360
    STG_ORD & STG_ITEMS --> MART_SALES
```

---

## 📊 Models

### Staging Models
Staging models perform minimal transformations — they are a 1:1 mapping from source with:
- Column renaming for consistency
- Type casting
- Null handling
- Timestamp standardization

### Mart Models

**`mart_daily_sales`** — Daily revenue aggregation
- Total orders, total revenue, average order value
- Day-over-day and week-over-week growth
- Running monthly total

**`mart_customer_360`** — Customer analytics
- Total orders, total spend, average order value
- First and last order dates
- Customer lifetime (days)
- Segment classification

---

## 🚀 Running

```bash
cd data-warehouse/dbt

# Install dependencies
dbt deps

# Compile models (dry run)
dbt compile

# Run all models
dbt run

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

---

## 📋 Schema Tests

Defined in `schema.yml` files:

| Test | Applied To | Purpose |
|:---|:---|:---|
| `unique` | Primary keys | No duplicate records |
| `not_null` | Required columns | Data completeness |
| `accepted_values` | Status, segment | Valid enum values |
| `relationships` | Foreign keys | Referential integrity |

---

## 🗃️ Migration

The initial schema migration (`V001__initial_schema.sql`) creates:
- Dimension tables with appropriate indexes
- Fact tables with foreign key constraints
- Date dimension with pre-populated values
- Materialized view stubs for common queries
