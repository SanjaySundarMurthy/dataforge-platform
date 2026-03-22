"""
Tests for DataForge API
========================
"""

import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "dataforge-api"


def test_metrics():
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "dataforge_api" in response.text


def test_docs():
    response = client.get("/docs")
    assert response.status_code == 200
