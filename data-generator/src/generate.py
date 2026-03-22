"""
DataForge — Realistic E-Commerce Data Generator
=================================================
Generates realistic sample data for the DataForge e-commerce
analytics platform.

Outputs data as CSV files to the landing zone and optionally
loads into PostgreSQL.

Concepts covered:
- Data generation with Faker
- Realistic distribution patterns
- CSV output with proper schemas
- Database loading with psycopg2
"""

import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# ── Configuration ────────────────────────────────────────────
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "./data/landing")
NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", "1000"))
NUM_PRODUCTS = int(os.getenv("NUM_PRODUCTS", "500"))
NUM_ORDERS = int(os.getenv("NUM_ORDERS", "10000"))
DAYS_BACK = int(os.getenv("DAYS_BACK", "365"))

# ── Realistic Data Templates ────────────────────────────────
CATEGORIES = {
    "electronics": ["smartphones", "laptops", "headphones", "cameras", "tablets"],
    "clothing": ["tops", "bottoms", "outerwear", "shoes", "accessories"],
    "home": ["furniture", "kitchen", "bedding", "lighting", "decor"],
    "sports": ["fitness", "outdoor", "team-sports", "yoga", "running"],
    "books": ["fiction", "non-fiction", "technical", "children", "academic"],
}

BRANDS = {
    "electronics": ["TechCo", "GadgetPro", "DigiMax", "SmartLife", "NeoTech"],
    "clothing": ["UrbanWear", "ClassicFit", "StyleX", "ComfortZone", "TrendSet"],
    "home": ["HomeBliss", "CozyLiving", "ModernSpace", "NaturalHome", "EliteDecor"],
    "sports": ["FitGear", "ActiveLife", "ProSport", "EnduranceX", "FlexZone"],
    "books": ["ReadMore", "PageTurner", "WisdomPress", "LearnHub", "BookVault"],
}

SEGMENTS = ["premium", "standard", "budget"]
REGIONS = ["east", "west", "north", "south", "central"]
COUNTRIES = ["US", "UK", "Canada", "Germany", "France", "Australia"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
ORDER_STATUSES = ["completed", "completed", "completed", "delivered", "shipped", "pending", "processing", "cancelled"]
EVENT_TYPES = ["page_view", "page_view", "page_view", "search", "product_view", "add_to_cart", "checkout", "purchase"]
DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["chrome", "firefox", "safari", "edge"]


def ensure_output_dir():
    """Create output directories."""
    Path(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    for table in ["customers", "products", "orders", "order_items", "clickstream", "reviews"]:
        (Path(OUTPUT_PATH) / table).mkdir(parents=True, exist_ok=True)


def generate_customers() -> list[dict]:
    """Generate realistic customer data."""
    print(f"  Generating {NUM_CUSTOMERS:,} customers...")
    customers = []

    for i in range(NUM_CUSTOMERS):
        customer = {
            "customer_id": f"C{i+1:06d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "segment": random.choices(SEGMENTS, weights=[20, 50, 30])[0],
            "region": random.choice(REGIONS),
            "city": fake.city(),
            "country": random.choice(COUNTRIES),
            "created_at": fake.date_time_between(
                start_date=f"-{DAYS_BACK * 2}d", end_date="-1d"
            ).isoformat(),
        }
        customers.append(customer)

    return customers


def generate_products() -> list[dict]:
    """Generate realistic product catalog."""
    print(f"  Generating {NUM_PRODUCTS:,} products...")
    products = []

    for i in range(NUM_PRODUCTS):
        category = random.choice(list(CATEGORIES.keys()))
        subcategory = random.choice(CATEGORIES[category])
        brand = random.choice(BRANDS[category])

        # Price distribution: mostly 10-200, some expensive items
        price = round(random.lognormvariate(3.5, 1.0), 2)
        price = max(5.0, min(price, 2000.0))
        cost = round(price * random.uniform(0.3, 0.7), 2)

        product = {
            "product_id": f"P{i+1:06d}",
            "product_name": f"{brand} {fake.word().title()} {subcategory.title()}",
            "category": category,
            "subcategory": subcategory,
            "brand": brand,
            "price": price,
            "cost": cost,
            "weight_kg": round(random.uniform(0.1, 20.0), 2),
            "created_at": fake.date_time_between(
                start_date=f"-{DAYS_BACK * 2}d", end_date="-30d"
            ).isoformat(),
        }
        products.append(product)

    return products


def generate_orders(customers: list, products: list) -> tuple[list, list]:
    """Generate realistic order data with line items."""
    print(f"  Generating {NUM_ORDERS:,} orders with items...")
    orders = []
    order_items = []
    item_counter = 0

    customer_ids = [c["customer_id"] for c in customers]
    product_list = products

    for i in range(NUM_ORDERS):
        customer_id = random.choice(customer_ids)
        order_date = fake.date_time_between(
            start_date=f"-{DAYS_BACK}d", end_date="now"
        )
        status = random.choice(ORDER_STATUSES)

        # Generate 1-5 line items per order
        num_items = random.choices([1, 2, 3, 4, 5], weights=[30, 35, 20, 10, 5])[0]
        order_total = 0.0

        for j in range(num_items):
            item_counter += 1
            product = random.choice(product_list)
            quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
            discount = random.choices(
                [0, 0.05, 0.10, 0.15, 0.20, 0.25],
                weights=[60, 15, 10, 8, 5, 2]
            )[0]
            unit_price = product["price"]
            line_total = round(quantity * unit_price * (1 - discount), 2)
            order_total += line_total

            item = {
                "item_id": f"I{item_counter:08d}",
                "order_id": f"O{i+1:08d}",
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": unit_price,
                "discount": discount,
            }
            order_items.append(item)

        shipping = round(random.uniform(0, 25.0), 2) if order_total < 100 else 0.0

        order = {
            "order_id": f"O{i+1:08d}",
            "customer_id": customer_id,
            "order_date": order_date.isoformat(),
            "status": status,
            "total_amount": round(order_total + shipping, 2),
            "shipping_cost": shipping,
            "payment_method": random.choice(PAYMENT_METHODS),
            "shipping_address": fake.address().replace("\n", ", "),
        }
        orders.append(order)

    return orders, order_items


def generate_clickstream(customers: list) -> list[dict]:
    """Generate clickstream / user behavior events."""
    num_events = NUM_ORDERS * 5  # ~5 events per order
    print(f"  Generating {num_events:,} clickstream events...")
    events = []
    customer_ids = [c["customer_id"] for c in customers]

    pages = ["/", "/products", "/cart", "/checkout", "/account", "/search", "/category/electronics",
             "/category/clothing", "/product/detail", "/about", "/contact"]

    for i in range(num_events):
        event = {
            "event_id": str(uuid.uuid4()),
            "session_id": str(uuid.uuid4())[:8],
            "customer_id": random.choice(customer_ids) if random.random() > 0.3 else "",
            "event_type": random.choice(EVENT_TYPES),
            "page_url": random.choice(pages),
            "referrer": random.choice(["google", "direct", "facebook", "email", "twitter", ""]),
            "device_type": random.choice(DEVICES),
            "browser": random.choice(BROWSERS),
            "timestamp": fake.date_time_between(
                start_date=f"-{DAYS_BACK}d", end_date="now"
            ).isoformat(),
        }
        events.append(event)

    return events


def generate_reviews(customers: list, products: list) -> list[dict]:
    """Generate product reviews."""
    num_reviews = NUM_ORDERS // 3  # ~1/3 of orders get a review
    print(f"  Generating {num_reviews:,} reviews...")
    reviews = []

    customer_ids = [c["customer_id"] for c in customers]
    product_ids = [p["product_id"] for p in products]

    for i in range(num_reviews):
        # Ratings follow a J-curve (many 4-5, fewer 1-3)
        rating = random.choices([1, 2, 3, 4, 5], weights=[5, 8, 15, 32, 40])[0]

        review = {
            "review_id": f"R{i+1:07d}",
            "product_id": random.choice(product_ids),
            "customer_id": random.choice(customer_ids),
            "rating": rating,
            "review_text": fake.paragraph(nb_sentences=random.randint(1, 5)),
            "helpful_votes": random.randint(0, 50),
            "created_at": fake.date_time_between(
                start_date=f"-{DAYS_BACK}d", end_date="now"
            ).isoformat(),
        }
        reviews.append(review)

    return reviews


def write_csv(data: list[dict], table_name: str):
    """Write data to CSV files in the landing zone."""
    if not data:
        return

    filepath = Path(OUTPUT_PATH) / table_name / f"{table_name}.csv"
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    print(f"  📄 Written {len(data):,} rows → {filepath}")


def load_to_postgres(data: list[dict], table_name: str):
    """Optionally load data into PostgreSQL raw schema."""
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "dataforge"),
            user=os.getenv("POSTGRES_USER", "dataforge_admin"),
            password=os.getenv("POSTGRES_PASSWORD", "changeme_in_production"),
        )

        with conn.cursor() as cur:
            columns = data[0].keys()
            col_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))

            cur.execute(f"DROP TABLE IF EXISTS raw.{table_name}")
            # Create table with text columns (raw layer)
            col_defs = ", ".join([f"{c} TEXT" for c in columns])
            cur.execute(f"CREATE TABLE raw.{table_name} ({col_defs})")

            for row in data:
                values = [str(row[c]) if row[c] is not None else None for c in columns]
                cur.execute(
                    f"INSERT INTO raw.{table_name} ({col_str}) VALUES ({placeholders})",
                    values,
                )

        conn.commit()
        conn.close()
        print(f"  🗄️  Loaded {len(data):,} rows → raw.{table_name}")

    except Exception as e:
        print(f"  ⚠️  PostgreSQL load skipped: {e}")


def main():
    """Generate all sample data."""
    print("=" * 60)
    print("🎲 DataForge Data Generator")
    print("=" * 60)
    print(f"  Customers: {NUM_CUSTOMERS:,}")
    print(f"  Products:  {NUM_PRODUCTS:,}")
    print(f"  Orders:    {NUM_ORDERS:,}")
    print(f"  Output:    {OUTPUT_PATH}")
    print("=" * 60)

    ensure_output_dir()

    # Generate data
    customers = generate_customers()
    products = generate_products()
    orders, order_items = generate_orders(customers, products)
    clickstream = generate_clickstream(customers)
    reviews = generate_reviews(customers, products)

    # Write to CSV (landing zone)
    print("\n📁 Writing CSV files...")
    write_csv(customers, "customers")
    write_csv(products, "products")
    write_csv(orders, "orders")
    write_csv(order_items, "order_items")
    write_csv(clickstream, "clickstream")
    write_csv(reviews, "reviews")

    # Load to PostgreSQL
    print("\n🗄️  Loading to PostgreSQL...")
    for name, data in [
        ("customers", customers),
        ("products", products),
        ("orders", orders),
        ("order_items", order_items),
        ("clickstream", clickstream),
        ("reviews", reviews),
    ]:
        load_to_postgres(data, name)

    print("\n" + "=" * 60)
    print("🎉 Data generation complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
