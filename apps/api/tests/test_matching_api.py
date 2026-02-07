"""Endpoint tests for POST /admin/matching/run."""

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from transit_api.main import app
from transit_api.services.matching.engine import MatchingReport


@pytest.fixture
def mock_db_connection() -> Any:
    """Mock database connection check."""
    with patch("transit_api.main.check_database_connection", new_callable=AsyncMock) as mock:
        mock.return_value = True
        yield mock


@pytest.fixture
async def client(mock_db_connection: Any) -> AsyncClient:  # noqa: ARG001
    """Async HTTP client for testing."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac  # type: ignore[misc]


class TestMatchingEndpoint:
    """Tests for POST /admin/matching/run."""

    @pytest.mark.asyncio
    async def test_success_response_schema(self, client: AsyncClient) -> None:
        """Success path returns all expected fields."""
        mock_report = MatchingReport(
            run_id="test-run-id",
            started_at="2026-02-06T08:00:00+00:00",
            ended_at="2026-02-06T08:00:01+00:00",
            duration_ms=1000,
            scanned_count=100,
            matched_count=80,
            unmatched_count=15,
            ambiguous_count=3,
            deduped_count=2,
            error_count=0,
        )

        with patch("transit_api.routers.admin.MatchingEngine") as mock_engine_cls:
            mock_engine = mock_engine_cls.return_value
            mock_engine.run = AsyncMock(return_value=mock_report)

            resp = await client.post("/admin/matching/run", json={})

        assert resp.status_code == 200
        data = resp.json()
        assert data["run_id"] == "test-run-id"
        assert data["scanned_count"] == 100
        assert data["matched_count"] == 80
        assert data["unmatched_count"] == 15
        assert data["ambiguous_count"] == 3
        assert data["deduped_count"] == 2
        assert data["error_count"] == 0
        assert data["duration_ms"] == 1000

    @pytest.mark.asyncio
    async def test_custom_parameters(self, client: AsyncClient) -> None:
        """Custom request parameters are forwarded to engine."""
        mock_report = MatchingReport()
        mock_report.started_at = "2026-02-06T08:00:00+00:00"
        mock_report.ended_at = "2026-02-06T08:00:01+00:00"

        with patch("transit_api.routers.admin.MatchingEngine") as mock_engine_cls:
            mock_engine = mock_engine_cls.return_value
            mock_engine.run = AsyncMock(return_value=mock_report)

            resp = await client.post(
                "/admin/matching/run",
                json={
                    "window_minutes": 60,
                    "max_candidates": 3,
                    "batch_size": 500,
                    "strict_mode": True,
                },
            )

        assert resp.status_code == 200
        mock_engine_cls.assert_called_once_with(
            window_minutes=60,
            max_candidates=3,
            batch_size=500,
            strict_mode=True,
        )

    @pytest.mark.asyncio
    async def test_engine_failure_returns_500(self, client: AsyncClient) -> None:
        """Engine exception -> 500."""
        with patch("transit_api.routers.admin.MatchingEngine") as mock_engine_cls:
            mock_engine = mock_engine_cls.return_value
            mock_engine.run = AsyncMock(side_effect=RuntimeError("DB connection lost"))

            resp = await client.post("/admin/matching/run", json={})

        assert resp.status_code == 500

    @pytest.mark.asyncio
    async def test_invalid_window_minutes(self, client: AsyncClient) -> None:
        """window_minutes=0 is rejected by validation."""
        resp = await client.post(
            "/admin/matching/run",
            json={"window_minutes": 0},
        )
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_default_parameters(self, client: AsyncClient) -> None:
        """Empty body uses defaults (no error)."""
        mock_report = MatchingReport()
        mock_report.started_at = "2026-02-06T08:00:00+00:00"
        mock_report.ended_at = "2026-02-06T08:00:01+00:00"

        with patch("transit_api.routers.admin.MatchingEngine") as mock_engine_cls:
            mock_engine = mock_engine_cls.return_value
            mock_engine.run = AsyncMock(return_value=mock_report)

            resp = await client.post("/admin/matching/run", json={})

        assert resp.status_code == 200
        mock_engine_cls.assert_called_once_with(
            window_minutes=None,
            max_candidates=None,
            batch_size=None,
            strict_mode=None,
        )
