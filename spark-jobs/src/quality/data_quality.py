"""
Data Quality Checks
====================
Validates data at each layer of the Medallion Architecture.

Concepts covered:
- Schema validation
- Null checks
- Range validation
- Uniqueness constraints
- Referential integrity
- Freshness checks
- Custom business rules
"""

from dataclasses import dataclass
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.logger import get_logger

logger = get_logger(__name__)


@dataclass
class QualityCheckResult:
    """Result of a single quality check."""

    check_name: str
    table_name: str
    layer: str
    passed: bool
    details: str
    rows_checked: int
    rows_failed: int = 0

    @property
    def pass_rate(self) -> float:
        if self.rows_checked == 0:
            return 100.0
        return round((self.rows_checked - self.rows_failed) / self.rows_checked * 100, 2)


class DataQualityChecker:
    """
    Data quality checker for DataForge pipelines.

    Usage:
        checker = DataQualityChecker(spark)
        results = checker.run_checks("silver/orders", [
            checker.check_not_null("order_id"),
            checker.check_unique("order_id"),
            checker.check_range("total_amount", min_val=0),
        ])
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.results: List[QualityCheckResult] = []

    def check_not_null(
        self, df: DataFrame, column: str, table_name: str, layer: str
    ) -> QualityCheckResult:
        """Check that a column has no null values."""
        total = df.count()
        nulls = df.filter(F.col(column).isNull()).count()

        result = QualityCheckResult(
            check_name=f"not_null:{column}",
            table_name=table_name,
            layer=layer,
            passed=nulls == 0,
            details=f"{nulls} null values found in '{column}'",
            rows_checked=total,
            rows_failed=nulls,
        )
        self.results.append(result)
        return result

    def check_unique(
        self, df: DataFrame, column: str, table_name: str, layer: str
    ) -> QualityCheckResult:
        """Check that a column has all unique values."""
        total = df.count()
        distinct = df.select(column).distinct().count()
        duplicates = total - distinct

        result = QualityCheckResult(
            check_name=f"unique:{column}",
            table_name=table_name,
            layer=layer,
            passed=duplicates == 0,
            details=f"{duplicates} duplicate values found in '{column}'",
            rows_checked=total,
            rows_failed=duplicates,
        )
        self.results.append(result)
        return result

    def check_range(
        self,
        df: DataFrame,
        column: str,
        table_name: str,
        layer: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> QualityCheckResult:
        """Check that numeric values fall within a range."""
        total = df.count()
        conditions = []

        if min_val is not None:
            conditions.append(F.col(column) < min_val)
        if max_val is not None:
            conditions.append(F.col(column) > max_val)

        if not conditions:
            return QualityCheckResult(
                check_name=f"range:{column}",
                table_name=table_name,
                layer=layer,
                passed=True,
                details="No range specified",
                rows_checked=total,
            )

        combined = conditions[0]
        for c in conditions[1:]:
            combined = combined | c

        violations = df.filter(combined).count()

        result = QualityCheckResult(
            check_name=f"range:{column}[{min_val},{max_val}]",
            table_name=table_name,
            layer=layer,
            passed=violations == 0,
            details=f"{violations} values out of range [{min_val}, {max_val}]",
            rows_checked=total,
            rows_failed=violations,
        )
        self.results.append(result)
        return result

    def check_referential_integrity(
        self,
        df: DataFrame,
        column: str,
        ref_df: DataFrame,
        ref_column: str,
        table_name: str,
        layer: str,
    ) -> QualityCheckResult:
        """Check that all values in column exist in reference table."""
        total = df.select(column).distinct().count()

        orphans = (
            df.select(column)
            .distinct()
            .join(ref_df.select(F.col(ref_column).alias(column)), column, "left_anti")
            .count()
        )

        result = QualityCheckResult(
            check_name=f"ref_integrity:{column}→{ref_column}",
            table_name=table_name,
            layer=layer,
            passed=orphans == 0,
            details=f"{orphans} orphan records found",
            rows_checked=total,
            rows_failed=orphans,
        )
        self.results.append(result)
        return result

    def check_freshness(
        self,
        df: DataFrame,
        timestamp_column: str,
        table_name: str,
        layer: str,
        max_hours: int = 24,
    ) -> QualityCheckResult:
        """Check that data is not stale."""
        latest = df.agg(F.max(timestamp_column)).collect()[0][0]

        if latest is None:
            return QualityCheckResult(
                check_name=f"freshness:{timestamp_column}",
                table_name=table_name,
                layer=layer,
                passed=False,
                details="No data found",
                rows_checked=0,
            )

        from datetime import datetime, timedelta
        cutoff = datetime.now() - timedelta(hours=max_hours)
        is_fresh = latest >= cutoff

        result = QualityCheckResult(
            check_name=f"freshness:{timestamp_column}(<{max_hours}h)",
            table_name=table_name,
            layer=layer,
            passed=is_fresh,
            details=f"Latest record: {latest}, Cutoff: {cutoff}",
            rows_checked=df.count(),
        )
        self.results.append(result)
        return result

    def check_column_values(
        self,
        df: DataFrame,
        column: str,
        allowed_values: list,
        table_name: str,
        layer: str,
    ) -> QualityCheckResult:
        """Check that column only contains allowed values."""
        total = df.count()
        invalid = df.filter(~F.col(column).isin(allowed_values)).count()

        result = QualityCheckResult(
            check_name=f"allowed_values:{column}",
            table_name=table_name,
            layer=layer,
            passed=invalid == 0,
            details=f"{invalid} rows with invalid values in '{column}'",
            rows_checked=total,
            rows_failed=invalid,
        )
        self.results.append(result)
        return result

    def get_summary(self) -> dict:
        """Get a summary of all quality checks."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed

        return {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": round(passed / total * 100, 2) if total > 0 else 0,
            "failures": [
                {"check": r.check_name, "table": r.table_name, "details": r.details}
                for r in self.results if not r.passed
            ],
        }

    def print_report(self) -> None:
        """Print a formatted quality report."""
        summary = self.get_summary()

        logger.info("=" * 60)
        logger.info("DATA QUALITY REPORT")
        logger.info("=" * 60)
        logger.info(f"Total checks: {summary['total_checks']}")
        logger.info(f"Passed:       {summary['passed']}")
        logger.info(f"Failed:       {summary['failed']}")
        logger.info(f"Pass rate:    {summary['pass_rate']}%")

        if summary["failures"]:
            logger.warning("FAILURES:")
            for f in summary["failures"]:
                logger.warning(f"  ❌ {f['check']} on {f['table']}: {f['details']}")
        else:
            logger.info("✅ All checks passed!")

        logger.info("=" * 60)


def run_quality_checks(spark: SparkSession, silver_path: str) -> dict:
    """Run all quality checks on Silver layer data."""
    checker = DataQualityChecker(spark)

    # Load Silver tables
    orders = spark.read.format("delta").load(f"{silver_path}/orders")
    customers = spark.read.format("delta").load(f"{silver_path}/customers")
    products = spark.read.format("delta").load(f"{silver_path}/products")
    order_items = spark.read.format("delta").load(f"{silver_path}/order_items")

    # Orders checks
    checker.check_not_null(orders, "order_id", "orders", "silver")
    checker.check_unique(orders, "order_id", "orders", "silver")
    checker.check_range(orders, "total_amount", "orders", "silver", min_val=0)
    checker.check_column_values(
        orders, "status", ["completed", "delivered", "shipped", "pending", "processing", "cancelled", "unknown"],
        "orders", "silver"
    )
    checker.check_referential_integrity(
        orders, "customer_id", customers, "customer_id", "orders", "silver"
    )

    # Customers checks
    checker.check_not_null(customers, "customer_id", "customers", "silver")
    checker.check_unique(customers, "customer_id", "customers", "silver")

    # Products checks
    checker.check_not_null(products, "product_id", "products", "silver")
    checker.check_unique(products, "product_id", "products", "silver")
    checker.check_range(products, "price", "products", "silver", min_val=0)
    checker.check_range(products, "profit_margin", "products", "silver", min_val=-100, max_val=100)

    # Order items checks
    checker.check_not_null(order_items, "item_id", "order_items", "silver")
    checker.check_range(order_items, "quantity", "order_items", "silver", min_val=1)
    checker.check_referential_integrity(
        order_items, "order_id", orders, "order_id", "order_items", "silver"
    )
    checker.check_referential_integrity(
        order_items, "product_id", products, "product_id", "order_items", "silver"
    )

    checker.print_report()
    return checker.get_summary()
