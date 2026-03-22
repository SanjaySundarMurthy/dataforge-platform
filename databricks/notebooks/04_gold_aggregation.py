# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold Aggregation
# MAGIC
# MAGIC **DataForge Platform** — Silver → Gold
# MAGIC
# MAGIC Creates business-level aggregations and analytics-ready datasets.
# MAGIC
# MAGIC ### Concepts Covered
# MAGIC - Complex aggregations with GROUP BY
# MAGIC - Window functions (running totals, moving averages, ranking)
# MAGIC - Multi-table JOINs
# MAGIC - RFM Customer Segmentation
# MAGIC - Star Schema preparation

# COMMAND ----------

dbutils.widgets.text("aggregation", "all", "Aggregation (daily_sales|customer_360|product_performance|all)")
dbutils.widgets.text("silver_path", "/mnt/datalake/silver", "Silver Path")
dbutils.widgets.text("gold_path", "/mnt/datalake/gold", "Gold Path")

aggregation = dbutils.widgets.get("aggregation")
silver_path = dbutils.widgets.get("silver_path")
gold_path = dbutils.widgets.get("gold_path")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Sales

# COMMAND ----------

def build_daily_sales():
    orders = spark.read.format("delta").load(f"{silver_path}/orders")

    daily = (
        orders
        .filter(F.col("status") != "cancelled")
        .groupBy(F.to_date("order_date").alias("sale_date"), "order_year", "order_month")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("total_amount").alias("gross_revenue"),
            F.sum("net_amount").alias("net_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
        )
    )

    # Running totals
    w_ytd = Window.partitionBy("order_year").orderBy("sale_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    w_7d = Window.orderBy("sale_date").rowsBetween(-6, Window.currentRow)

    daily = (
        daily
        .withColumn("ytd_revenue", F.sum("gross_revenue").over(w_ytd))
        .withColumn("moving_avg_7d", F.avg("gross_revenue").over(w_7d))
        .withColumn("_gold_timestamp", F.current_timestamp())
    )

    daily.write.format("delta").mode("overwrite").save(f"{gold_path}/daily_sales")
    print(f"✅ daily_sales: {daily.count():,} rows")
    return daily

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer 360

# COMMAND ----------

def build_customer_360():
    customers = spark.read.format("delta").load(f"{silver_path}/customers")
    orders = spark.read.format("delta").load(f"{silver_path}/orders")
    reviews = spark.read.format("delta").load(f"{silver_path}/reviews")

    # Order metrics
    order_metrics = (
        orders
        .filter(F.col("status") != "cancelled")
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_spend"),
            F.avg("total_amount").alias("avg_order_value"),
            F.max("order_date").alias("last_order_date"),
            F.datediff(F.current_date(), F.max("order_date")).alias("days_since_last_order"),
        )
    )

    # Review metrics
    review_metrics = (
        reviews
        .groupBy("customer_id")
        .agg(F.count("*").alias("total_reviews"), F.avg("rating").alias("avg_rating"))
    )

    # Combine with RFM scoring
    c360 = (
        customers
        .join(order_metrics, "customer_id", "left")
        .join(review_metrics, "customer_id", "left")
        .fillna(0, subset=["total_orders", "total_spend", "total_reviews"])
        .withColumn("rfm_recency",
            F.when(F.col("days_since_last_order") <= 30, 5)
            .when(F.col("days_since_last_order") <= 90, 4)
            .when(F.col("days_since_last_order") <= 180, 3)
            .when(F.col("days_since_last_order") <= 365, 2)
            .otherwise(1))
        .withColumn("rfm_frequency",
            F.when(F.col("total_orders") >= 20, 5)
            .when(F.col("total_orders") >= 10, 4)
            .when(F.col("total_orders") >= 5, 3)
            .when(F.col("total_orders") >= 2, 2)
            .otherwise(1))
        .withColumn("customer_tier",
            F.when((F.col("rfm_recency") >= 4) & (F.col("rfm_frequency") >= 4), "champion")
            .when(F.col("rfm_recency") >= 3, "loyal")
            .when(F.col("rfm_recency") <= 2, "at_risk")
            .otherwise("regular"))
        .withColumn("_gold_timestamp", F.current_timestamp())
    )

    c360.write.format("delta").mode("overwrite").save(f"{gold_path}/customer_360")
    print(f"✅ customer_360: {c360.count():,} rows")
    return c360

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Performance

# COMMAND ----------

def build_product_performance():
    products = spark.read.format("delta").load(f"{silver_path}/products")
    order_items = spark.read.format("delta").load(f"{silver_path}/order_items")
    reviews = spark.read.format("delta").load(f"{silver_path}/reviews")

    sales = (
        order_items
        .groupBy("product_id")
        .agg(
            F.sum("line_total").alias("total_revenue"),
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("order_id").alias("order_count"),
        )
    )

    review_agg = (
        reviews
        .groupBy("product_id")
        .agg(F.avg("rating").alias("avg_rating"), F.count("*").alias("review_count"))
    )

    # Rank within category
    w = Window.partitionBy("category").orderBy(F.col("total_revenue").desc())

    perf = (
        products
        .join(sales, "product_id", "left")
        .join(review_agg, "product_id", "left")
        .fillna(0, subset=["total_revenue", "units_sold", "review_count"])
        .withColumn("category_rank", F.rank().over(w))
        .withColumn("_gold_timestamp", F.current_timestamp())
    )

    perf.write.format("delta").mode("overwrite").save(f"{gold_path}/product_performance")
    print(f"✅ product_performance: {perf.count():,} rows")
    return perf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute

# COMMAND ----------

builders = {
    "daily_sales": build_daily_sales,
    "customer_360": build_customer_360,
    "product_performance": build_product_performance,
}

if aggregation == "all":
    for name, fn in builders.items():
        print(f"\n{'='*50}")
        print(f"Building: {name}")
        fn()
elif aggregation in builders:
    builders[aggregation]()
else:
    raise ValueError(f"Unknown aggregation: {aggregation}")

print("\n🎉 Gold aggregations complete!")
