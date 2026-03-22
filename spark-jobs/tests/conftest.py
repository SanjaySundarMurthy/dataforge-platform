"""
Pytest Fixtures for Spark Tests
================================
Provides shared SparkSession and sample data for all tests.

Concepts covered:
- Pytest fixtures (session, function scope)
- SparkSession for testing
- Test data factories
"""

import os
import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType,
)


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for the test session."""
    os.environ["ENVIRONMENT"] = "local"

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("DataForge-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_customers(spark):
    """Sample customer data for testing."""
    data = [
        ("C001", "John", "Doe", "john@example.com", "+1234", "premium", "east", "NYC", "US", datetime(2023, 1, 15)),
        ("C002", "Jane", "Smith", "jane@example.com", "+5678", "standard", "west", "LA", "US", datetime(2023, 3, 20)),
        ("C003", "Bob", "Wilson", "invalid-email", "+9012", "  PREMIUM  ", "east", "Boston", "US", datetime(2023, 6, 1)),
        ("C004", "Alice", "Brown", "alice@example.com", None, None, None, None, None, datetime(2023, 9, 10)),
        ("C001", "John", "Doe", "john@example.com", "+1234", "premium", "east", "NYC", "US", datetime(2023, 1, 15)),  # duplicate
    ]

    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("segment", StringType()),
        StructField("region", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("created_at", TimestampType()),
    ])

    from pyspark.sql import functions as F
    return spark.createDataFrame(data, schema).withColumn("_ingestion_timestamp", F.current_timestamp())


@pytest.fixture
def sample_orders(spark):
    """Sample order data for testing."""
    now = datetime.now()
    data = [
        ("O001", "C001", now - timedelta(days=10), "completed", 150.0, 10.0, "credit_card", "123 Main St"),
        ("O002", "C002", now - timedelta(days=5), "shipped", 250.0, 15.0, "paypal", "456 Oak Ave"),
        ("O003", "C001", now - timedelta(days=2), "pending", 75.0, 5.0, "credit_card", "123 Main St"),
        ("O004", "C003", now - timedelta(days=1), "cancelled", 300.0, 20.0, "debit_card", "789 Pine Rd"),
        ("O005", "C002", now - timedelta(days=30), "delivered", -10.0, 0.0, "credit_card", "456 Oak Ave"),  # negative amount
    ]

    schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_date", TimestampType()),
        StructField("status", StringType()),
        StructField("total_amount", DoubleType()),
        StructField("shipping_cost", DoubleType()),
        StructField("payment_method", StringType()),
        StructField("shipping_address", StringType()),
    ])

    from pyspark.sql import functions as F
    return spark.createDataFrame(data, schema).withColumn("_ingestion_timestamp", F.current_timestamp())


@pytest.fixture
def sample_products(spark):
    """Sample product data for testing."""
    data = [
        ("P001", "Widget A", "electronics", "gadgets", "Acme", 99.99, 45.0, 0.5, datetime(2023, 1, 1)),
        ("P002", "Widget B", "electronics", "gadgets", "Acme", 199.99, 90.0, 1.0, datetime(2023, 2, 1)),
        ("P003", "Shirt", "clothing", "tops", "BrandX", 29.99, 10.0, 0.3, datetime(2023, 3, 1)),
        ("P004", "Bad Product", "test", "test", "Test", -5.0, -2.0, 0.1, datetime(2023, 4, 1)),  # negative price
    ]

    schema = StructType([
        StructField("product_id", StringType()),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("subcategory", StringType()),
        StructField("brand", StringType()),
        StructField("price", DoubleType()),
        StructField("cost", DoubleType()),
        StructField("weight_kg", DoubleType()),
        StructField("created_at", TimestampType()),
    ])

    from pyspark.sql import functions as F
    return spark.createDataFrame(data, schema).withColumn("_ingestion_timestamp", F.current_timestamp())
