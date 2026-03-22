"""
Tests for Data Quality Checker
================================
Validates that quality checks correctly identify issues.
"""

import pytest
from pyspark.sql import functions as F

from src.quality.data_quality import DataQualityChecker


class TestNotNullCheck:
    def test_passes_when_no_nulls(self, spark, sample_customers):
        checker = DataQualityChecker(spark)
        result = checker.check_not_null(sample_customers, "customer_id", "customers", "test")
        assert result.passed is True

    def test_fails_when_nulls_exist(self, spark, sample_customers):
        checker = DataQualityChecker(spark)
        result = checker.check_not_null(sample_customers, "phone", "customers", "test")
        assert result.passed is False
        assert result.rows_failed > 0


class TestUniqueCheck:
    def test_fails_with_duplicates(self, spark, sample_customers):
        checker = DataQualityChecker(spark)
        result = checker.check_unique(sample_customers, "customer_id", "customers", "test")
        assert result.passed is False  # C001 is duplicated

    def test_passes_with_unique_values(self, spark, sample_orders):
        checker = DataQualityChecker(spark)
        result = checker.check_unique(sample_orders, "order_id", "orders", "test")
        assert result.passed is True


class TestRangeCheck:
    def test_passes_when_all_in_range(self, spark, sample_orders):
        positive_orders = sample_orders.filter(F.col("total_amount") >= 0)
        checker = DataQualityChecker(spark)
        result = checker.check_range(positive_orders, "total_amount", "orders", "test", min_val=0)
        assert result.passed is True

    def test_fails_when_out_of_range(self, spark, sample_orders):
        checker = DataQualityChecker(spark)
        result = checker.check_range(sample_orders, "total_amount", "orders", "test", min_val=0)
        assert result.passed is False


class TestColumnValuesCheck:
    def test_passes_with_valid_values(self, spark, sample_orders):
        checker = DataQualityChecker(spark)
        result = checker.check_column_values(
            sample_orders, "status",
            ["completed", "shipped", "pending", "cancelled", "delivered"],
            "orders", "test"
        )
        assert result.passed is True

    def test_fails_with_invalid_values(self, spark, sample_orders):
        checker = DataQualityChecker(spark)
        result = checker.check_column_values(
            sample_orders, "status",
            ["completed"],  # Only allow 'completed'
            "orders", "test"
        )
        assert result.passed is False


class TestQualitySummary:
    def test_summary_counts(self, spark, sample_orders):
        checker = DataQualityChecker(spark)
        checker.check_not_null(sample_orders, "order_id", "orders", "test")
        checker.check_unique(sample_orders, "order_id", "orders", "test")

        summary = checker.get_summary()
        assert summary["total_checks"] == 2
        assert summary["passed"] == 2
        assert summary["failed"] == 0
        assert summary["pass_rate"] == 100.0
