# 🎲 Data Generator — E-Commerce Simulator

> Generates realistic e-commerce data using Faker for 6 source tables.

---

## 📁 Structure

```
data-generator/
└── src/
    └── generate.py    # Main generator script (CLI-enabled)
```

---

## 📊 Generated Tables

| Table | Rows (default) | Key Fields |
|:---|:---|:---|
| `customers` | 1000 | customer_id, name, email, segment, city, country |
| `products` | 500 | product_id, name, category, brand, price |
| `orders` | 5000 | order_id, customer_id, order_date, total, status |
| `order_items` | 15000 | item_id, order_id, product_id, quantity, price |
| `clickstream` | 20000 | session_id, customer_id (nullable), page, action, timestamp |
| `reviews` | 3000 | review_id, product_id, customer_id, rating, text |

---

## 🚀 Usage

```bash
# Install dependencies
pip install faker

# Generate with defaults
python data-generator/src/generate.py

# Generate with custom parameters
python data-generator/src/generate.py \
  --output-dir ./data/landing \
  --rows 10000 \
  --customers 2000 \
  --products 800

# Arguments:
#   --output-dir   Where to write CSV files (default: data/landing)
#   --rows         Base row count for orders (default: 5000)
#   --customers    Number of customers (default: 1000)
#   --products     Number of products (default: 500)
```

---

## 🔑 Data Characteristics

- **Realistic distributions:** Customer segments follow 80/20 patterns
- **Referential integrity:** All FKs reference valid parent records
- **Null handling:** Clickstream `customer_id` is `None` for anonymous sessions (not empty string)
- **Date ranges:** Orders span the last 2 years with realistic seasonal patterns
- **Varied statuses:** Orders include completed, pending, cancelled, refunded
