"""
Spark Session Factory
====================
Creates and configures SparkSession for different environments.

Concepts covered:
- SparkSession builder pattern
- Delta Lake integration
- Configuration management
- Hive metastore support
"""

import os
from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "DataForge-ETL",
    environment: str = None,
    extra_configs: dict = None,
) -> SparkSession:
    """
    Create a configured SparkSession.

    Args:
        app_name: Name of the Spark application
        environment: Runtime environment (local, dev, staging, prod)
        extra_configs: Additional Spark configurations

    Returns:
        Configured SparkSession
    """
    environment = environment or os.getenv("ENVIRONMENT", "local")

    builder = (
        SparkSession.builder
        .appName(f"{app_name}-{environment}")
        # Delta Lake support
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0"
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        # Performance tuning
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "auto")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    # Environment-specific configs
    if environment == "local":
        builder = (
            builder
            .master("local[*]")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        )

    # Merge extra configs
    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()
