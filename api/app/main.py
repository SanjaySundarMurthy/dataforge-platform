"""
DataForge API — Main Application
==================================
FastAPI analytics service exposing data warehouse insights.

Concepts covered:
- FastAPI application structure
- Dependency injection
- Pydantic models for validation
- Health checks & readiness probes
- Prometheus metrics integration
- CORS configuration
"""

import os
from contextlib import asynccontextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest
from fastapi.responses import PlainTextResponse

from app.models import (
    HealthResponse,
    DailySalesResponse,
    Customer360Response,
    ProductPerformanceResponse,
    PipelineStatusResponse,
)


# ── Metrics ──────────────────────────────────────────────────
REQUEST_COUNT = Counter(
    "dataforge_api_requests_total",
    "Total API requests",
    ["method", "endpoint", "status"],
)
REQUEST_LATENCY = Histogram(
    "dataforge_api_request_duration_seconds",
    "Request latency",
    ["endpoint"],
)


# ── Database Connection ──────────────────────────────────────
def get_db():
    """Database dependency — yields a connection per request."""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "dataforge"),
        user=os.getenv("POSTGRES_USER", "dataforge_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "changeme_in_production"),
        cursor_factory=RealDictCursor,
    )
    try:
        yield conn
    finally:
        conn.close()


# ── Application ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup/shutdown events."""
    print("🚀 DataForge API starting...")
    yield
    print("👋 DataForge API shutting down...")


app = FastAPI(
    title="DataForge Analytics API",
    description="REST API for DataForge Platform — E-Commerce Analytics",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Health & Readiness ───────────────────────────────────────
@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Liveness probe — is the service running?"""
    return HealthResponse(status="healthy", service="dataforge-api", version="1.0.0")


@app.get("/ready", tags=["Health"])
async def readiness_check(db=Depends(get_db)):
    """Readiness probe — can the service handle requests?"""
    try:
        with db.cursor() as cur:
            cur.execute("SELECT 1")
        return {"status": "ready", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Not ready: {e}")


# ── Metrics Endpoint ─────────────────────────────────────────
@app.get("/metrics", tags=["Monitoring"])
async def metrics():
    """Prometheus metrics endpoint."""
    return PlainTextResponse(generate_latest(), media_type="text/plain")


# ── Analytics Endpoints ──────────────────────────────────────
@app.get("/api/v1/sales/daily", response_model=list[DailySalesResponse], tags=["Analytics"])
async def get_daily_sales(
    limit: int = Query(default=30, ge=1, le=365),
    db=Depends(get_db),
):
    """Get daily sales data with running totals."""
    with db.cursor() as cur:
        cur.execute(
            """
            SELECT sale_date, total_orders, unique_customers,
                   gross_revenue, net_revenue, avg_order_value,
                   ytd_revenue, moving_avg_7d_revenue
            FROM analytics.daily_sales
            ORDER BY sale_date DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    REQUEST_COUNT.labels("GET", "/api/v1/sales/daily", "200").inc()
    return [DailySalesResponse(**row) for row in rows]


@app.get("/api/v1/customers", response_model=list[Customer360Response], tags=["Analytics"])
async def get_customers(
    tier: str = Query(default=None, description="Filter by customer tier"),
    limit: int = Query(default=50, ge=1, le=500),
    db=Depends(get_db),
):
    """Get customer 360 profiles with RFM segmentation."""
    with db.cursor() as cur:
        query = """
            SELECT customer_id, full_name, segment, region,
                   total_orders, total_spend, avg_order_value,
                   customer_tier, rfm_recency, rfm_frequency, rfm_monetary
            FROM analytics.customer_360
        """
        params = []

        if tier:
            query += " WHERE customer_tier = %s"
            params.append(tier)

        query += " ORDER BY total_spend DESC LIMIT %s"
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()

    REQUEST_COUNT.labels("GET", "/api/v1/customers", "200").inc()
    return [Customer360Response(**row) for row in rows]


@app.get("/api/v1/products", response_model=list[ProductPerformanceResponse], tags=["Analytics"])
async def get_products(
    category: str = Query(default=None, description="Filter by category"),
    limit: int = Query(default=50, ge=1, le=500),
    db=Depends(get_db),
):
    """Get product performance metrics with rankings."""
    with db.cursor() as cur:
        query = """
            SELECT product_id, product_name, category, brand,
                   total_revenue, units_sold, order_count,
                   category_revenue_rank, overall_revenue_rank
            FROM analytics.product_performance
        """
        params = []

        if category:
            query += " WHERE category = %s"
            params.append(category)

        query += " ORDER BY total_revenue DESC LIMIT %s"
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()

    REQUEST_COUNT.labels("GET", "/api/v1/products", "200").inc()
    return [ProductPerformanceResponse(**row) for row in rows]


@app.get("/api/v1/pipeline/status", response_model=list[PipelineStatusResponse], tags=["Pipeline"])
async def get_pipeline_status(
    limit: int = Query(default=10, ge=1, le=100),
    db=Depends(get_db),
):
    """Get recent pipeline execution status."""
    with db.cursor() as cur:
        cur.execute(
            """
            SELECT pipeline_name, table_name, rows_processed,
                   status, execution_time, created_at
            FROM raw.pipeline_log
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    return [PipelineStatusResponse(**row) for row in rows]
