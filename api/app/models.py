"""
Pydantic Models for DataForge API
===================================
Response models for API endpoints.

Concepts covered:
- Pydantic BaseModel for validation
- Optional fields with defaults
- DateTime serialization
"""

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str


class DailySalesResponse(BaseModel):
    sale_date: date
    total_orders: int
    unique_customers: int
    gross_revenue: float
    net_revenue: float
    avg_order_value: float
    ytd_revenue: Optional[float] = None
    moving_avg_7d_revenue: Optional[float] = None


class Customer360Response(BaseModel):
    customer_id: str
    full_name: Optional[str] = None
    segment: Optional[str] = None
    region: Optional[str] = None
    total_orders: int = 0
    total_spend: float = 0.0
    avg_order_value: float = 0.0
    customer_tier: Optional[str] = None
    rfm_recency: Optional[int] = None
    rfm_frequency: Optional[int] = None
    rfm_monetary: Optional[int] = None


class ProductPerformanceResponse(BaseModel):
    product_id: str
    product_name: Optional[str] = None
    category: Optional[str] = None
    brand: Optional[str] = None
    total_revenue: float = 0.0
    units_sold: int = 0
    order_count: int = 0
    category_revenue_rank: Optional[int] = None
    overall_revenue_rank: Optional[int] = None


class PipelineStatusResponse(BaseModel):
    pipeline_name: Optional[str] = None
    table_name: Optional[str] = None
    rows_processed: Optional[int] = None
    status: Optional[str] = None
    execution_time: Optional[int] = None
    created_at: Optional[datetime] = None
