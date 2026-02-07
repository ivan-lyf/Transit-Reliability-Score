"""Tests for health endpoint."""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_endpoint_returns_200(client: AsyncClient) -> None:
    """Test that health endpoint returns 200 with expected fields."""
    response = await client.get("/health")

    assert response.status_code == 200

    data = response.json()
    assert data["service"] == "Transit Reliability Score API"
    assert data["status"] == "healthy"
    assert "version" in data
    assert "environment" in data
    assert "timestamp" in data
    assert "checks" in data
    assert isinstance(data["checks"]["database"], bool)
    assert isinstance(data["checks"]["gtfsRt"], dict)
    assert "workerRunning" in data["checks"]["gtfsRt"]
    assert "pollCount" in data["checks"]["gtfsRt"]
    assert "issues" in data
    assert isinstance(data["issues"], list)


@pytest.mark.asyncio
async def test_health_endpoint_includes_version(client: AsyncClient) -> None:
    """Test that health endpoint includes app version."""
    response = await client.get("/health")
    data = response.json()

    assert data["version"] == "0.1.0"


@pytest.mark.asyncio
async def test_health_endpoint_includes_environment(client: AsyncClient) -> None:
    """Test that health endpoint includes environment."""
    response = await client.get("/health")
    data = response.json()

    assert data["environment"] in ["development", "staging", "production"]


@pytest.mark.asyncio
async def test_health_endpoint_has_request_id_header(client: AsyncClient) -> None:
    """Test that health endpoint response includes X-Request-ID header."""
    response = await client.get("/health")

    assert "X-Request-ID" in response.headers
    assert len(response.headers["X-Request-ID"]) > 0


@pytest.mark.asyncio
async def test_attribution_endpoint(client: AsyncClient) -> None:
    """Test that attribution endpoint returns required TransLink attribution."""
    response = await client.get("/meta/attribution")

    assert response.status_code == 200

    data = response.json()
    assert "attribution" in data
    assert "termsUrl" in data
    assert "TransLink" in data["attribution"]
    assert data["termsUrl"].startswith("https://")
