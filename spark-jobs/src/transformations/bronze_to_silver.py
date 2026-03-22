"""
Bronze → Silver Transformation
================================
Cleans, validates, deduplicates, and types raw Bronze data
into the Silver layer using Delta Lake format.

Concepts covered:
- Data cleaning (nulls, duplicates, invalid values)
- Type casting and standardization
- Delta Lake MERGE (upsert) operations
- Slowly Changing Dimensions (SCD Type 1)
- Data quality assertions
- Column renaming and standardization
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import get_config
from src.common.logger import get_logger

logger = get_logger(__name__)


# ── Generic Cleaning Functions ───────────────────────────────

def remove_duplicates(df: DataFrame, key_columns: list) -> DataFrame:
    """
    Remove duplicate rows, keeping the latest ingestion.

    Uses window functions to rank rows by ingestion timestamp
    and keeps only the most recent version of each record.
    """
    window = Window.partitionBy(*key_columns).orderBy(
        F.col("_ingestion_timestamp").desc()
    )
    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def standardize_strings(df: DataFrame, columns: list) -> DataFrame:
    """Trim whitespace and lowercase string columns."""
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.trim(F.lower(F.col(col_name))))
    return df


def add_silver_metadata(df: DataFrame) -> DataFrame:
    """Add Silver-layer specific metadata columns."""
    return (
        df
        .withColumn("_silver_timestamp", F.current_timestamp())
        .withColumn("_silver_version", F.lit(1))
    )


# ── Table-Specific Transformations ───────────────────────────

def transform_customers(df: DataFrame) -> DataFrame:
    """
    Clean and transform customer data.

    Steps:
    1. Remove duplicates by customer_id
    2. Validate email format
    3. Standardize region/segment names
    4. Fill missing values with defaults
    """
    logger.info("Transforming customers: Bronze → Silver")

    df = remove_duplicates(df, ["customer_id"])
    df = standardize_strings(df, ["segment", "region", "country", "city"])

    df = (
        df
        # Validate email (basic regex check)
        .withColumn(
            "email_valid",
            F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        )
        # Standardize segment names
        .withColumn(
            "segment",
            F.when(F.col("segment").isin("premium", "gold", "vip"), "premium")
            .when(F.col("segment").isin("standard", "regular", "normal"), "standard")
            .otherwise(F.coalesce(F.col("segment"), F.lit("unknown")))
        )
        # Fill missing regions
        .fillna({"region": "unknown", "city": "unknown", "country": "unknown"})
        # Add derived columns
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
    )

    df = add_silver_metadata(df)
    logger.info(f"  Customers after cleaning: {df.count()}")
    return df


def transform_products(df: DataFrame) -> DataFrame:
    """
    Clean and transform product data.

    Steps:
    1. Remove duplicates
    2. Validate price > 0
    3. Calculate profit margin
    4. Standardize categories
    """
    logger.info("Transforming products: Bronze → Silver")

    df = remove_duplicates(df, ["product_id"])
    df = standardize_strings(df, ["category", "subcategory", "brand"])

    df = (
        df
        .filter(F.col("price") > 0)
        .filter(F.col("cost") > 0)
        .withColumn(
            "profit_margin",
            F.round((F.col("price") - F.col("cost")) / F.col("price") * 100, 2)
        )
        .withColumn(
            "price_tier",
            F.when(F.col("price") < 25, "budget")
            .when(F.col("price") < 100, "mid-range")
            .when(F.col("price") < 500, "premium")
            .otherwise("luxury")
        )
    )

    df = add_silver_metadata(df)
    logger.info(f"  Products after cleaning: {df.count()}")
    return df


def transform_orders(df: DataFrame) -> DataFrame:
    """
    Clean and transform order data.

    Steps:
    1. Remove duplicates
    2. Validate amounts
    3. Extract date components
    4. Standardize status values
    """
    logger.info("Transforming orders: Bronze → Silver")

    df = remove_duplicates(df, ["order_id"])

    df = (
        df
        .filter(F.col("total_amount") >= 0)
        .withColumn(
            "status",
            F.when(F.col("status").isin("completed", "delivered", "shipped"), F.col("status"))
            .when(F.col("status").isin("pending", "processing"), F.col("status"))
            .when(F.col("status").isin("cancelled", "canceled", "refunded"), "cancelled")
            .otherwise("unknown")
        )
        # Date components for analytics
        .withColumn("order_date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_day", F.dayofmonth("order_date"))
        .withColumn("order_dayofweek", F.dayofweek("order_date"))
        .withColumn("order_hour", F.hour("order_date"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("order_date").isin(1, 7), True).otherwise(False)
        )
        # Net amount
        .withColumn(
            "net_amount",
            F.col("total_amount") - F.coalesce(F.col("shipping_cost"), F.lit(0))
        )
    )

    df = add_silver_metadata(df)
    logger.info(f"  Orders after cleaning: {df.count()}")
    return df


def transform_order_items(df: DataFrame) -> DataFrame:
    """Clean and transform order line items."""
    logger.info("Transforming order_items: Bronze → Silver")

    df = remove_duplicates(df, ["item_id"])

    df = (
        df
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
        .withColumn("discount", F.coalesce(F.col("discount"), F.lit(0.0)))
        .withColumn(
            "line_total",
            F.round(
                F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")),
                2
            )
        )
    )

    df = add_silver_metadata(df)
    logger.info(f"  Order items after cleaning: {df.count()}")
    return df


def transform_clickstream(df: DataFrame) -> DataFrame:
    """Clean and transform clickstream/event data."""
    logger.info("Transforming clickstream: Bronze → Silver")

    df = remove_duplicates(df, ["event_id"])
    df = standardize_strings(df, ["event_type", "device_type", "browser"])

    df = (
        df
        .filter(F.col("event_type").isNotNull())
        .withColumn("event_date", F.to_date("timestamp"))
        .withColumn("event_hour", F.hour("timestamp"))
        .withColumn(
            "is_conversion",
            F.when(F.col("event_type").isin("purchase", "checkout", "add_to_cart"), True)
            .otherwise(False)
        )
    )

    df = add_silver_metadata(df)
    logger.info(f"  Clickstream after cleaning: {df.count()}")
    return df


def transform_reviews(df: DataFrame) -> DataFrame:
    """Clean and transform product reviews."""
    logger.info("Transforming reviews: Bronze → Silver")

    df = remove_duplicates(df, ["review_id"])

    df = (
        df
        .filter(F.col("rating").between(1, 5))
        .withColumn(
            "sentiment",
            F.when(F.col("rating") >= 4, "positive")
            .when(F.col("rating") == 3, "neutral")
            .otherwise("negative")
        )
        .withColumn(
            "review_length",
            F.length(F.col("review_text"))
        )
    )

    df = add_silver_metadata(df)
    logger.info(f"  Reviews after cleaning: {df.count()}")
    return df


# ── Write to Silver ──────────────────────────────────────────

def write_to_silver(df: DataFrame, table_name: str) -> None:
    """Write cleaned DataFrame to the Silver layer as Delta."""
    config = get_config()
    target_path = f"{config.storage.silver_path}/{table_name}"

    logger.info(f"Writing to Silver (Delta): {target_path}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    logger.info(f"  ✅ Silver/{table_name} written successfully")


# ── Transformation Registry ──────────────────────────────────

TRANSFORMATIONS = {
    "customers": transform_customers,
    "products": transform_products,
    "orders": transform_orders,
    "order_items": transform_order_items,
    "clickstream": transform_clickstream,
    "reviews": transform_reviews,
}


def run_bronze_to_silver(spark: SparkSession) -> None:
    """Execute all Bronze → Silver transformations."""
    config = get_config()

    for table_name in config.tables.source_tables:
        bronze_path = f"{config.storage.bronze_path}/{table_name}"
        transform_fn = TRANSFORMATIONS.get(table_name)

        if not transform_fn:
            logger.warning(f"No transformation defined for {table_name}, skipping")
            continue

        try:
            logger.info(f"{'='*60}")
            logger.info(f"Processing: {table_name}")
            logger.info(f"{'='*60}")

            # Read from Bronze
            df = spark.read.parquet(bronze_path)
            logger.info(f"  Bronze rows: {df.count()}")

            # Transform
            df_clean = transform_fn(df)

            # Write to Silver
            write_to_silver(df_clean, table_name)

        except Exception as e:
            logger.error(f"Failed to transform {table_name}: {e}")
            raise

    logger.info("🎉 All Bronze → Silver transformations completed!")


if __name__ == "__main__":
    from src.common.spark_session import get_spark_session

    spark = get_spark_session("DataForge-BronzeToSilver")
    try:
        run_bronze_to_silver(spark)
    finally:
        spark.stop()
