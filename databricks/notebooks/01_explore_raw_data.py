# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Explore Raw Data
# MAGIC
# MAGIC **DataForge Platform** — Data Exploration Notebook
# MAGIC
# MAGIC This notebook is the starting point for understanding our e-commerce data.
# MAGIC We'll explore each source table to understand:
# MAGIC - Schema and data types
# MAGIC - Row counts and distributions
# MAGIC - Data quality issues
# MAGIC - Key patterns and relationships
# MAGIC
# MAGIC ### Concepts Covered
# MAGIC - `display()` for interactive visualization
# MAGIC - `describe()` for summary statistics
# MAGIC - Spark SQL for ad-hoc analysis
# MAGIC - Data profiling techniques

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F

# Configuration
BRONZE_PATH = spark.conf.get("spark.databricks.bronze_path", "/mnt/datalake/bronze")

# List available tables
tables = ["customers", "products", "orders", "order_items", "clickstream", "reviews"]

for table in tables:
    try:
        df = spark.read.parquet(f"{BRONZE_PATH}/{table}")
        print(f"✅ {table}: {df.count():,} rows, {len(df.columns)} columns")
    except Exception as e:
        print(f"❌ {table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore Customers

# COMMAND ----------

customers = spark.read.parquet(f"{BRONZE_PATH}/customers")
display(customers.limit(10))

# COMMAND ----------

# Summary statistics
display(customers.describe())

# COMMAND ----------

# Segment distribution
display(
    customers
    .groupBy("segment")
    .agg(
        F.count("*").alias("count"),
        F.countDistinct("region").alias("regions"),
    )
    .orderBy(F.col("count").desc())
)

# COMMAND ----------

# Regional distribution
display(
    customers
    .groupBy("region", "country")
    .count()
    .orderBy(F.col("count").desc())
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore Orders

# COMMAND ----------

orders = spark.read.parquet(f"{BRONZE_PATH}/orders")
display(orders.limit(10))

# COMMAND ----------

# Order status distribution
display(
    orders
    .groupBy("status")
    .agg(
        F.count("*").alias("order_count"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
    )
)

# COMMAND ----------

# Daily order trend
display(
    orders
    .withColumn("order_date", F.to_date("order_date"))
    .groupBy("order_date")
    .agg(
        F.count("*").alias("orders"),
        F.sum("total_amount").alias("revenue"),
    )
    .orderBy("order_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Overview

# COMMAND ----------

# Check nulls across all tables
for table in tables:
    try:
        df = spark.read.parquet(f"{BRONZE_PATH}/{table}")
        total = df.count()
        null_counts = []
        for col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            if null_count > 0:
                null_counts.append(f"  {col_name}: {null_count} ({null_count/total*100:.1f}%)")

        print(f"\n📊 {table} ({total:,} rows)")
        if null_counts:
            print("  Null columns:")
            for nc in null_counts:
                print(nc)
        else:
            print("  ✅ No null values")
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Relationships

# COMMAND ----------

# Verify referential integrity
orders = spark.read.parquet(f"{BRONZE_PATH}/orders")
customers = spark.read.parquet(f"{BRONZE_PATH}/customers")

orphan_orders = (
    orders.select("customer_id").distinct()
    .join(customers.select("customer_id").distinct(), "customer_id", "left_anti")
)

print(f"Orphan orders (no matching customer): {orphan_orders.count()}")
