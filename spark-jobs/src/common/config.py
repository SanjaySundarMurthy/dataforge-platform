"""
Configuration Management
========================
Centralized configuration for all Spark jobs.

Concepts covered:
- Environment-based configuration
- Path management for Medallion Architecture
- Configuration dataclass patterns
"""

import os
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class StorageConfig:
    """Storage paths for the Medallion Architecture."""

    base_path: str = ""
    bronze_path: str = ""
    silver_path: str = ""
    gold_path: str = ""
    landing_path: str = ""
    checkpoint_path: str = ""

    def __post_init__(self):
        if not self.base_path:
            env = os.getenv("ENVIRONMENT", "local")
            if env == "local":
                self.base_path = os.getenv("DATA_PATH", "/data")
            else:
                storage_account = os.getenv("STORAGE_ACCOUNT_NAME", "")
                self.base_path = f"abfss://{{container}}@{storage_account}.dfs.core.windows.net"

        if not self.landing_path:
            self.landing_path = self._resolve("landing")
        if not self.bronze_path:
            self.bronze_path = self._resolve("bronze")
        if not self.silver_path:
            self.silver_path = self._resolve("silver")
        if not self.gold_path:
            self.gold_path = self._resolve("gold")
        if not self.checkpoint_path:
            self.checkpoint_path = f"{self.bronze_path}/_checkpoints"

    def _resolve(self, layer: str) -> str:
        env = os.getenv("ENVIRONMENT", "local")
        if env == "local":
            return f"{self.base_path}/{layer}"
        return self.base_path.format(container=layer)


@dataclass
class TableConfig:
    """Table definitions for each layer."""

    # Source tables
    source_tables: list = field(default_factory=lambda: [
        "customers",
        "products",
        "orders",
        "order_items",
        "clickstream",
        "reviews",
    ])

    # Silver layer schemas
    silver_tables: Dict[str, str] = field(default_factory=lambda: {
        "customers": "silver/customers",
        "products": "silver/products",
        "orders": "silver/orders",
        "order_items": "silver/order_items",
        "clickstream": "silver/clickstream",
        "reviews": "silver/reviews",
    })

    # Gold layer aggregations
    gold_tables: Dict[str, str] = field(default_factory=lambda: {
        "daily_sales": "gold/daily_sales",
        "customer_360": "gold/customer_360",
        "product_performance": "gold/product_performance",
        "hourly_traffic": "gold/hourly_traffic",
    })


@dataclass
class PipelineConfig:
    """Complete pipeline configuration."""

    storage: StorageConfig = field(default_factory=StorageConfig)
    tables: TableConfig = field(default_factory=TableConfig)
    environment: str = field(default_factory=lambda: os.getenv("ENVIRONMENT", "local"))
    batch_date: str = field(default_factory=lambda: os.getenv("BATCH_DATE", ""))
    parallelism: int = field(default_factory=lambda: int(os.getenv("PARALLELISM", "4")))


def get_config() -> PipelineConfig:
    """Get the pipeline configuration for the current environment."""
    return PipelineConfig()
