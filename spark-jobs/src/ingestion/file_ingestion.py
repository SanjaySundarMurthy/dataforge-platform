"""
File Ingestion — Landing Zone → Bronze Layer
=============================================
Reads raw files (CSV, JSON, Parquet) from the landing zone
and writes them to the Bronze layer as-is with metadata.

Concepts covered:
- Reading multiple file formats with Spark
- Schema inference vs explicit schemas
- Adding audit/metadata columns
- Partitioned writes
- Idempotent ingestion (overwrite partition)
"""

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, LongType,
)

from src.common.config import get_config
from src.common.logger import get_logger

logger = get_logger(__name__)


# ── Explicit Schemas (best practice for production) ──────────
SCHEMAS = {
    "customers": StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("region", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]),
    "products": StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("cost", DoubleType(), True),
        StructField("weight_kg", DoubleType(), True),
        StructField("created_at", TimestampType(), True),
    ]),
    "orders": StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("shipping_cost", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("shipping_address", StringType(), True),
    ]),
    "order_items": StructType([
        StructField("item_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
    ]),
    "clickstream": StructType([
        StructField("event_id", StringType(), False),
        StructField("session_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("timestamp", TimestampType(), True),
    ]),
    "reviews": StructType([
        StructField("review_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("rating", IntegerType(), True),
        StructField("review_text", StringType(), True),
        StructField("helpful_votes", LongType(), True),
        StructField("created_at", TimestampType(), True),
    ]),
}


def add_metadata_columns(df: DataFrame, source: str) -> DataFrame:
    """
    Add audit/metadata columns to a DataFrame.

    This is a best practice for data lineage and debugging.
    Every record in the Bronze layer should have:
    - _ingestion_timestamp: When it was ingested
    - _source_file: Which file it came from
    - _source_system: Which system produced the data
    """
    return (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_source_system", F.lit(source))
    )


def ingest_csv(
    spark: SparkSession,
    source_path: str,
    table_name: str,
    schema: StructType = None,
) -> DataFrame:
    """
    Ingest a CSV file into a Spark DataFrame.

    Args:
        spark: SparkSession
        source_path: Path to the CSV file(s)
        table_name: Name of the source table
        schema: Optional explicit schema

    Returns:
        DataFrame with metadata columns added
    """
    logger.info(f"Ingesting CSV: {source_path} → {table_name}")

    reader = spark.read.option("header", "true").option("inferSchema", "false")

    if schema:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", "true")

    df = reader.csv(source_path)
    df = add_metadata_columns(df, source=f"csv/{table_name}")

    logger.info(f"  Rows read: {df.count()}, Columns: {len(df.columns)}")
    return df


def ingest_json(
    spark: SparkSession,
    source_path: str,
    table_name: str,
    schema: StructType = None,
) -> DataFrame:
    """Ingest a JSON file into a Spark DataFrame."""
    logger.info(f"Ingesting JSON: {source_path} → {table_name}")

    reader = spark.read.option("multiline", "true")

    if schema:
        reader = reader.schema(schema)

    df = reader.json(source_path)
    df = add_metadata_columns(df, source=f"json/{table_name}")

    logger.info(f"  Rows read: {df.count()}, Columns: {len(df.columns)}")
    return df


def write_to_bronze(
    df: DataFrame,
    table_name: str,
    partition_cols: list = None,
) -> None:
    """
    Write a DataFrame to the Bronze layer.

    Uses overwrite mode with dynamic partition overwrite
    for idempotent ingestion.

    Args:
        df: DataFrame to write
        table_name: Target table name
        partition_cols: Columns to partition by
    """
    config = get_config()
    target_path = f"{config.storage.bronze_path}/{table_name}"

    logger.info(f"Writing to Bronze: {target_path}")

    writer = df.write.mode("overwrite").format("parquet")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(target_path)
    logger.info(f"  ✅ Written to {target_path}")


def run_ingestion(spark: SparkSession) -> None:
    """
    Execute the full ingestion pipeline.
    Reads all source tables from landing zone → writes to Bronze.
    """
    config = get_config()

    for table_name in config.tables.source_tables:
        schema = SCHEMAS.get(table_name)
        source_path = f"{config.storage.landing_path}/{table_name}"

        try:
            df = ingest_csv(spark, source_path, table_name, schema)

            # Partition orders and clickstream by date
            partition_cols = None
            if table_name in ("orders", "clickstream"):
                date_col = "order_date" if table_name == "orders" else "timestamp"
                df = df.withColumn("_partition_date", F.to_date(F.col(date_col)))
                partition_cols = ["_partition_date"]

            write_to_bronze(df, table_name, partition_cols)
        except Exception as e:
            logger.error(f"Failed to ingest {table_name}: {e}")
            raise


if __name__ == "__main__":
    from src.common.spark_session import get_spark_session

    spark = get_spark_session("DataForge-Ingestion")
    try:
        run_ingestion(spark)
    finally:
        spark.stop()
