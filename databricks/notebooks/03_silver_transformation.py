# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver Transformation
# MAGIC
# MAGIC **DataForge Platform** — Bronze → Silver
# MAGIC
# MAGIC Cleans, validates, and transforms raw Bronze data into the Silver layer
# MAGIC using Delta Lake format with MERGE (upsert) operations.
# MAGIC
# MAGIC ### Concepts Covered
# MAGIC - Delta Lake MERGE for upserts (SCD Type 1)
# MAGIC - Data cleaning & validation
# MAGIC - Schema evolution
# MAGIC - Z-ORDER optimization
# MAGIC - OPTIMIZE and VACUUM

# COMMAND ----------

dbutils.widgets.text("table_name", "customers", "Table Name")
dbutils.widgets.text("bronze_path", "/mnt/datalake/bronze", "Bronze Path")
dbutils.widgets.text("silver_path", "/mnt/datalake/silver", "Silver Path")

table_name = dbutils.widgets.get("table_name")
bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Functions

# COMMAND ----------

def transform_customers(df):
    """Clean customer data."""
    return (
        df
        .dropDuplicates(["customer_id"])
        .withColumn("segment", F.trim(F.lower(F.col("segment"))))
        .withColumn("region", F.coalesce(F.trim(F.lower(F.col("region"))), F.lit("unknown")))
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("email_valid",
            F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))
        .withColumn("_silver_timestamp", F.current_timestamp())
    )

def transform_products(df):
    """Clean product data."""
    return (
        df
        .dropDuplicates(["product_id"])
        .filter(F.col("price") > 0)
        .filter(F.col("cost") > 0)
        .withColumn("profit_margin",
            F.round((F.col("price") - F.col("cost")) / F.col("price") * 100, 2))
        .withColumn("price_tier",
            F.when(F.col("price") < 25, "budget")
            .when(F.col("price") < 100, "mid-range")
            .when(F.col("price") < 500, "premium")
            .otherwise("luxury"))
        .withColumn("_silver_timestamp", F.current_timestamp())
    )

def transform_orders(df):
    """Clean order data."""
    return (
        df
        .dropDuplicates(["order_id"])
        .filter(F.col("total_amount") >= 0)
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("net_amount",
            F.col("total_amount") - F.coalesce(F.col("shipping_cost"), F.lit(0)))
        .withColumn("is_weekend",
            F.when(F.dayofweek("order_date").isin(1, 7), True).otherwise(False))
        .withColumn("_silver_timestamp", F.current_timestamp())
    )

def transform_order_items(df):
    """Clean order items data."""
    return (
        df
        .dropDuplicates(["item_id"])
        .filter(F.col("quantity") > 0)
        .withColumn("discount", F.coalesce(F.col("discount"), F.lit(0.0)))
        .withColumn("line_total",
            F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2))
        .withColumn("_silver_timestamp", F.current_timestamp())
    )

# Transformation registry
TRANSFORMS = {
    "customers": ("customer_id", transform_customers),
    "products": ("product_id", transform_products),
    "orders": ("order_id", transform_orders),
    "order_items": ("item_id", transform_order_items),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake MERGE (Upsert)
# MAGIC
# MAGIC Instead of overwriting, we use MERGE to:
# MAGIC - INSERT new records
# MAGIC - UPDATE existing records (SCD Type 1)
# MAGIC - Track changes efficiently

# COMMAND ----------

def upsert_to_silver(df, table, key_col):
    """Upsert (MERGE) data into Silver Delta table."""
    target_path = f"{silver_path}/{table}"

    if DeltaTable.isDeltaTable(spark, target_path):
        # MERGE: Update existing + Insert new
        delta_table = DeltaTable.forPath(spark, target_path)

        (
            delta_table.alias("target")
            .merge(df.alias("source"), f"target.{key_col} = source.{key_col}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"  ✅ MERGED into {target_path}")
    else:
        # First load: create the Delta table
        df.write.format("delta").mode("overwrite").save(target_path)
        print(f"  ✅ CREATED {target_path}")

    # Optimize the table
    spark.sql(f"OPTIMIZE delta.`{target_path}`")
    print(f"  📦 Optimized {target_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Transformation

# COMMAND ----------

if table_name in TRANSFORMS:
    key_col, transform_fn = TRANSFORMS[table_name]

    # Read Bronze
    bronze_df = spark.read.parquet(f"{bronze_path}/{table_name}")
    print(f"📥 Bronze rows: {bronze_df.count():,}")

    # Transform
    silver_df = transform_fn(bronze_df)
    print(f"🔄 Silver rows: {silver_df.count():,}")

    # Upsert to Silver
    upsert_to_silver(silver_df, table_name, key_col)
else:
    print(f"❌ Unknown table: {table_name}")

# COMMAND ----------

# Verify the result
result = spark.read.format("delta").load(f"{silver_path}/{table_name}")
print(f"\n📊 Silver/{table_name}: {result.count():,} rows")
display(result.limit(5))
