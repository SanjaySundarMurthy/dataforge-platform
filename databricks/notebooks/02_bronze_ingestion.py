# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze Ingestion
# MAGIC
# MAGIC **DataForge Platform** — Bronze Layer Ingestion
# MAGIC
# MAGIC Reads raw data from the landing zone and writes to the Bronze layer.
# MAGIC Bronze = raw data as-is with audit metadata columns.
# MAGIC
# MAGIC ### Concepts Covered
# MAGIC - Reading CSV/JSON/Parquet with explicit schemas
# MAGIC - Adding audit columns (_ingestion_timestamp, _source_file)
# MAGIC - Partitioned writes for efficient querying
# MAGIC - Auto Loader (Structured Streaming) for incremental ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for parameterization (used when called from ADF)
dbutils.widgets.text("landing_path", "/mnt/datalake/landing", "Landing Zone Path")
dbutils.widgets.text("bronze_path", "/mnt/datalake/bronze", "Bronze Path")
dbutils.widgets.text("table_name", "all", "Table Name (or 'all')")

landing_path = dbutils.widgets.get("landing_path")
bronze_path = dbutils.widgets.get("bronze_path")
table_name = dbutils.widgets.get("table_name")

print(f"Landing: {landing_path}")
print(f"Bronze:  {bronze_path}")
print(f"Table:   {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Ingestion

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

def ingest_to_bronze(table: str, schema=None):
    """Read from landing zone and write to bronze with metadata."""
    source = f"{landing_path}/{table}"
    target = f"{bronze_path}/{table}"

    print(f"📥 Ingesting: {source} → {target}")

    reader = spark.read.option("header", "true")
    if schema:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", "true")

    df = reader.csv(source)

    # Add audit columns
    df = (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(dbutils.widgets.get("table_name")))
    )

    # Write as Parquet
    df.write.mode("overwrite").parquet(target)

    count = df.count()
    print(f"  ✅ {count:,} rows written to {target}")
    return count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader (Streaming Ingestion)
# MAGIC
# MAGIC Auto Loader uses Structured Streaming to efficiently process
# MAGIC new files as they arrive in the landing zone.

# COMMAND ----------

def ingest_with_autoloader(table: str, schema=None):
    """
    Use Auto Loader for incremental file ingestion.
    This is the recommended approach for production workloads.
    """
    source = f"{landing_path}/{table}"
    target = f"{bronze_path}/{table}_streaming"
    checkpoint = f"{bronze_path}/_checkpoints/{table}"

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
        .option("header", "true")
    )

    if schema:
        reader = reader.schema(schema)
    else:
        reader = reader.option("cloudFiles.inferColumnTypes", "true")

    df = reader.load(source)

    # Add audit columns
    df = (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    # Write as Delta with streaming
    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start(target)
    )

    query.awaitTermination()
    print(f"  ✅ Auto Loader completed for {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Ingestion

# COMMAND ----------

tables = ["customers", "products", "orders", "order_items", "clickstream", "reviews"]

if table_name == "all":
    for t in tables:
        try:
            ingest_to_bronze(t)
        except Exception as e:
            print(f"  ❌ Failed: {t} - {e}")
else:
    ingest_to_bronze(table_name)

print("\n🎉 Bronze ingestion complete!")
