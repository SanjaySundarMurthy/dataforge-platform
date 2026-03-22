"""
Silver → Gold Transformation
==============================
Creates business-level aggregations and analytics-ready
datasets from the cleaned Silver layer.

Concepts covered:
- Multi-table JOINs
- Window functions for running totals
- Aggregations (GROUP BY, ROLLUP)
- Pivot tables
- Customer 360 view pattern
- KPI calculations
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import get_config
from src.common.logger import get_logger

logger = get_logger(__name__)


def build_daily_sales(spark: SparkSession, silver_path: str) -> DataFrame:
    """
    Build daily sales aggregation.

    This is the core business KPI table showing:
    - Revenue, order count, AOV per day
    - Running totals and moving averages
    - Year-over-year comparisons
    """
    logger.info("Building Gold: daily_sales")

    orders = spark.read.format("delta").load(f"{silver_path}/orders")
    order_items = spark.read.format("delta").load(f"{silver_path}/order_items")

    # Join orders with items for detailed metrics
    daily = (
        orders
        .filter(F.col("status") != "cancelled")
        .groupBy(
            F.to_date("order_date").alias("sale_date"),
            "order_year",
            "order_month",
            "order_day",
            "is_weekend",
        )
        .agg(
            F.count("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("total_amount").alias("gross_revenue"),
            F.sum("net_amount").alias("net_revenue"),
            F.sum("shipping_cost").alias("total_shipping"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("total_amount").alias("min_order_value"),
            F.max("total_amount").alias("max_order_value"),
        )
    )

    # Add running totals and moving averages (Window Functions)
    window_ytd = (
        Window
        .partitionBy("order_year")
        .orderBy("sale_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    window_7d = (
        Window
        .orderBy("sale_date")
        .rowsBetween(-6, Window.currentRow)
    )

    window_30d = (
        Window
        .orderBy("sale_date")
        .rowsBetween(-29, Window.currentRow)
    )

    daily = (
        daily
        .withColumn("ytd_revenue", F.sum("gross_revenue").over(window_ytd))
        .withColumn("ytd_orders", F.sum("total_orders").over(window_ytd))
        .withColumn("moving_avg_7d_revenue", F.avg("gross_revenue").over(window_7d))
        .withColumn("moving_avg_30d_revenue", F.avg("gross_revenue").over(window_30d))
        .withColumn("moving_avg_7d_orders", F.avg("total_orders").over(window_7d))
        .withColumn("_gold_timestamp", F.current_timestamp())
    )

    logger.info(f"  Daily sales rows: {daily.count()}")
    return daily


def build_customer_360(spark: SparkSession, silver_path: str) -> DataFrame:
    """
    Build Customer 360 view.

    A comprehensive customer profile combining:
    - Order history (total spend, frequency, recency)
    - RFM segmentation
    - Product preferences
    - Review activity
    - Clickstream engagement
    """
    logger.info("Building Gold: customer_360")

    customers = spark.read.format("delta").load(f"{silver_path}/customers")
    orders = spark.read.format("delta").load(f"{silver_path}/orders")
    order_items = spark.read.format("delta").load(f"{silver_path}/order_items")
    reviews = spark.read.format("delta").load(f"{silver_path}/reviews")

    # Order metrics per customer
    order_metrics = (
        orders
        .filter(F.col("status") != "cancelled")
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_spend"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.datediff(F.current_date(), F.max("order_date")).alias("days_since_last_order"),
        )
    )

    # Review metrics
    review_metrics = (
        reviews
        .groupBy("customer_id")
        .agg(
            F.count("review_id").alias("total_reviews"),
            F.avg("rating").alias("avg_rating_given"),
        )
    )

    # Favorite product category (most ordered)
    items_with_products = (
        order_items
        .join(
            spark.read.format("delta").load(f"{silver_path}/products")
            .select("product_id", "category"),
            "product_id"
        )
    )

    fav_category = (
        items_with_products
        .join(orders.select("order_id", "customer_id"), "order_id")
        .groupBy("customer_id", "category")
        .agg(F.count("*").alias("cat_count"))
    )

    window_cat = Window.partitionBy("customer_id").orderBy(F.col("cat_count").desc())
    fav_category = (
        fav_category
        .withColumn("rank", F.row_number().over(window_cat))
        .filter(F.col("rank") == 1)
        .select("customer_id", F.col("category").alias("favorite_category"))
    )

    # Combine everything
    customer_360 = (
        customers
        .join(order_metrics, "customer_id", "left")
        .join(review_metrics, "customer_id", "left")
        .join(fav_category, "customer_id", "left")
        .fillna(0, subset=["total_orders", "total_spend", "total_reviews"])
    )

    # RFM Segmentation
    # Recency: days since last order
    # Frequency: total orders
    # Monetary: total spend
    customer_360 = (
        customer_360
        .withColumn(
            "rfm_recency_score",
            F.when(F.col("days_since_last_order") <= 30, 5)
            .when(F.col("days_since_last_order") <= 90, 4)
            .when(F.col("days_since_last_order") <= 180, 3)
            .when(F.col("days_since_last_order") <= 365, 2)
            .otherwise(1)
        )
        .withColumn(
            "rfm_frequency_score",
            F.when(F.col("total_orders") >= 20, 5)
            .when(F.col("total_orders") >= 10, 4)
            .when(F.col("total_orders") >= 5, 3)
            .when(F.col("total_orders") >= 2, 2)
            .otherwise(1)
        )
        .withColumn(
            "rfm_monetary_score",
            F.when(F.col("total_spend") >= 5000, 5)
            .when(F.col("total_spend") >= 2000, 4)
            .when(F.col("total_spend") >= 500, 3)
            .when(F.col("total_spend") >= 100, 2)
            .otherwise(1)
        )
        .withColumn(
            "customer_tier",
            F.when(
                (F.col("rfm_recency_score") >= 4) &
                (F.col("rfm_frequency_score") >= 4) &
                (F.col("rfm_monetary_score") >= 4),
                "champion"
            )
            .when(
                (F.col("rfm_recency_score") >= 3) &
                (F.col("rfm_frequency_score") >= 3),
                "loyal"
            )
            .when(F.col("rfm_recency_score") >= 4, "new_active")
            .when(F.col("rfm_recency_score") <= 2, "at_risk")
            .otherwise("regular")
        )
        .withColumn(
            "lifetime_value_estimate",
            F.round(F.col("avg_order_value") * F.col("total_orders") * 1.2, 2)
        )
        .withColumn("_gold_timestamp", F.current_timestamp())
    )

    logger.info(f"  Customer 360 rows: {customer_360.count()}")
    return customer_360


def build_product_performance(spark: SparkSession, silver_path: str) -> DataFrame:
    """
    Build product performance KPIs.

    Metrics per product:
    - Revenue, units sold, order count
    - Average rating and review count
    - Return rate (cancelled orders)
    - Revenue rank within category
    """
    logger.info("Building Gold: product_performance")

    products = spark.read.format("delta").load(f"{silver_path}/products")
    orders = spark.read.format("delta").load(f"{silver_path}/orders")
    order_items = spark.read.format("delta").load(f"{silver_path}/order_items")
    reviews = spark.read.format("delta").load(f"{silver_path}/reviews")

    # Sales metrics
    sales = (
        order_items
        .join(orders.select("order_id", "status"), "order_id")
        .groupBy("product_id")
        .agg(
            F.sum(
                F.when(F.col("status") != "cancelled", F.col("line_total"))
                .otherwise(0)
            ).alias("total_revenue"),
            F.sum(
                F.when(F.col("status") != "cancelled", F.col("quantity"))
                .otherwise(0)
            ).alias("units_sold"),
            F.countDistinct(
                F.when(F.col("status") != "cancelled", F.col("order_id"))
            ).alias("order_count"),
            F.countDistinct(
                F.when(F.col("status") == "cancelled", F.col("order_id"))
            ).alias("cancelled_orders"),
        )
    )

    # Review metrics
    review_agg = (
        reviews
        .groupBy("product_id")
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.count("review_id").alias("review_count"),
            F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("positive_reviews"),
            F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("negative_reviews"),
        )
    )

    # Combine
    product_perf = (
        products
        .join(sales, "product_id", "left")
        .join(review_agg, "product_id", "left")
        .fillna(0, subset=["total_revenue", "units_sold", "order_count", "review_count"])
        .withColumn(
            "cancellation_rate",
            F.when(
                (F.col("order_count") + F.col("cancelled_orders")) > 0,
                F.round(
                    F.col("cancelled_orders") /
                    (F.col("order_count") + F.col("cancelled_orders")) * 100,
                    2
                )
            ).otherwise(0)
        )
    )

    # Revenue rank within category
    window_cat = Window.partitionBy("category").orderBy(F.col("total_revenue").desc())
    product_perf = (
        product_perf
        .withColumn("category_revenue_rank", F.rank().over(window_cat))
        .withColumn("_gold_timestamp", F.current_timestamp())
    )

    logger.info(f"  Product performance rows: {product_perf.count()}")
    return product_perf


def build_hourly_traffic(spark: SparkSession, silver_path: str) -> DataFrame:
    """
    Build hourly traffic analysis from clickstream data.

    Useful for understanding user behavior patterns,
    peak hours, and conversion funnels.
    """
    logger.info("Building Gold: hourly_traffic")

    clickstream = spark.read.format("delta").load(f"{silver_path}/clickstream")

    traffic = (
        clickstream
        .groupBy("event_date", "event_hour", "device_type")
        .agg(
            F.count("event_id").alias("total_events"),
            F.countDistinct("session_id").alias("unique_sessions"),
            F.countDistinct("customer_id").alias("unique_users"),
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_carts"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            F.sum(F.when(F.col("is_conversion"), 1).otherwise(0)).alias("conversions"),
        )
        .withColumn(
            "conversion_rate",
            F.when(F.col("unique_sessions") > 0,
                   F.round(F.col("conversions") / F.col("unique_sessions") * 100, 2))
            .otherwise(0)
        )
        .withColumn("_gold_timestamp", F.current_timestamp())
    )

    logger.info(f"  Hourly traffic rows: {traffic.count()}")
    return traffic


# ── Write to Gold ────────────────────────────────────────────

def write_to_gold(df: DataFrame, table_name: str) -> None:
    """Write aggregated DataFrame to the Gold layer as Delta."""
    config = get_config()
    target_path = f"{config.storage.gold_path}/{table_name}"

    logger.info(f"Writing to Gold (Delta): {target_path}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    logger.info(f"  ✅ Gold/{table_name} written successfully")


# ── Orchestrator ─────────────────────────────────────────────

GOLD_BUILDERS = {
    "daily_sales": build_daily_sales,
    "customer_360": build_customer_360,
    "product_performance": build_product_performance,
    "hourly_traffic": build_hourly_traffic,
}


def run_silver_to_gold(spark: SparkSession) -> None:
    """Execute all Silver → Gold aggregations."""
    config = get_config()

    for table_name, builder_fn in GOLD_BUILDERS.items():
        try:
            logger.info(f"{'='*60}")
            logger.info(f"Building Gold: {table_name}")
            logger.info(f"{'='*60}")

            df = builder_fn(spark, config.storage.silver_path)
            write_to_gold(df, table_name)

        except Exception as e:
            logger.error(f"Failed to build {table_name}: {e}")
            raise

    logger.info("🎉 All Silver → Gold aggregations completed!")


if __name__ == "__main__":
    from src.common.spark_session import get_spark_session

    spark = get_spark_session("DataForge-SilverToGold")
    try:
        run_silver_to_gold(spark)
    finally:
        spark.stop()
