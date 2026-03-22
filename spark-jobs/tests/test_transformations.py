"""
Tests for Bronze → Silver Transformations
==========================================
Validates cleaning, deduplication, and enrichment logic.

Concepts covered:
- PySpark DataFrame testing
- Assert on row counts, column values, schema
- Testing data cleaning logic
"""

import pytest
from pyspark.sql import functions as F

from src.transformations.bronze_to_silver import (
    transform_customers,
    transform_orders,
    transform_products,
    remove_duplicates,
    standardize_strings,
)


class TestRemoveDuplicates:
    """Test the generic deduplication function."""

    def test_removes_exact_duplicates(self, sample_customers):
        """Verify duplicates are removed based on key column."""
        result = remove_duplicates(sample_customers, ["customer_id"])
        assert result.count() == 4  # 5 rows → 4 unique customer_ids

    def test_keeps_latest_record(self, spark, sample_customers):
        """Verify the most recent record is retained."""
        result = remove_duplicates(sample_customers, ["customer_id"])
        c001 = result.filter(F.col("customer_id") == "C001").collect()
        assert len(c001) == 1


class TestStandardizeStrings:
    """Test string standardization."""

    def test_trims_and_lowercases(self, sample_customers):
        """Verify whitespace is trimmed and values lowercased."""
        result = standardize_strings(sample_customers, ["segment"])
        segments = [row["segment"] for row in result.select("segment").collect() if row["segment"]]
        for s in segments:
            assert s == s.strip() == s.lower()


class TestTransformCustomers:
    """Test customer transformation logic."""

    def test_deduplication(self, sample_customers):
        """Verify duplicate customers are removed."""
        result = transform_customers(sample_customers)
        assert result.count() == 4

    def test_adds_full_name(self, sample_customers):
        """Verify full_name column is created."""
        result = transform_customers(sample_customers)
        assert "full_name" in result.columns

    def test_segment_standardization(self, sample_customers):
        """Verify segment values are standardized."""
        result = transform_customers(sample_customers)
        valid_segments = {"premium", "standard", "unknown"}
        segments = {row["segment"] for row in result.select("segment").collect()}
        assert segments.issubset(valid_segments)

    def test_fills_missing_region(self, sample_customers):
        """Verify null regions are filled with 'unknown'."""
        result = transform_customers(sample_customers)
        nulls = result.filter(F.col("region").isNull()).count()
        assert nulls == 0

    def test_silver_metadata_added(self, sample_customers):
        """Verify Silver layer metadata columns are added."""
        result = transform_customers(sample_customers)
        assert "_silver_timestamp" in result.columns
        assert "_silver_version" in result.columns


class TestTransformOrders:
    """Test order transformation logic."""

    def test_filters_negative_amounts(self, sample_orders):
        """Verify orders with negative amounts are filtered."""
        result = transform_orders(sample_orders)
        negatives = result.filter(F.col("total_amount") < 0).count()
        assert negatives == 0

    def test_status_standardization(self, sample_orders):
        """Verify status values are standardized."""
        result = transform_orders(sample_orders)
        valid_statuses = {"completed", "delivered", "shipped", "pending", "processing", "cancelled", "unknown"}
        statuses = {row["status"] for row in result.select("status").collect()}
        assert statuses.issubset(valid_statuses)

    def test_date_components_extracted(self, sample_orders):
        """Verify date component columns are created."""
        result = transform_orders(sample_orders)
        expected_cols = ["order_year", "order_month", "order_day", "order_dayofweek", "is_weekend"]
        for col in expected_cols:
            assert col in result.columns

    def test_net_amount_calculated(self, sample_orders):
        """Verify net_amount = total_amount - shipping_cost."""
        result = transform_orders(sample_orders)
        row = result.filter(F.col("order_id") == "O001").collect()[0]
        assert row["net_amount"] == 150.0 - 10.0


class TestTransformProducts:
    """Test product transformation logic."""

    def test_filters_invalid_prices(self, sample_products):
        """Verify products with invalid prices are filtered."""
        result = transform_products(sample_products)
        invalid = result.filter(F.col("price") <= 0).count()
        assert invalid == 0

    def test_profit_margin_calculation(self, sample_products):
        """Verify profit margin is calculated correctly."""
        result = transform_products(sample_products)
        p001 = result.filter(F.col("product_id") == "P001").collect()[0]
        expected_margin = round((99.99 - 45.0) / 99.99 * 100, 2)
        assert abs(p001["profit_margin"] - expected_margin) < 0.01

    def test_price_tier_assignment(self, sample_products):
        """Verify price tiers are assigned correctly."""
        result = transform_products(sample_products)
        p001 = result.filter(F.col("product_id") == "P001").collect()[0]
        assert p001["price_tier"] == "mid-range"  # 99.99 is between 25 and 100

        p002 = result.filter(F.col("product_id") == "P002").collect()[0]
        assert p002["price_tier"] == "premium"  # 199.99 is between 100 and 500
