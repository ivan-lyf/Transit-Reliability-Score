"""Tests for admin GTFS import API endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from transit_api.main import app
from transit_api.services.gtfs_static.importer import ImportReport

from .fixtures.gtfs_fixture import build_gtfs_zip


@pytest.fixture
def mock_db_connection() -> Any:
    """Mock database connection check."""
    with patch("transit_api.main.check_database_connection", new_callable=AsyncMock) as mock:
        mock.return_value = True
        yield mock


@pytest.fixture
async def api_client(mock_db_connection: Any) -> AsyncClient:  # noqa: ARG001
    """Async HTTP client for testing."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


class TestAdminImportEndpoint:
    """Tests for POST /admin/import/static-gtfs."""

    async def test_success_dry_run(self, api_client: AsyncClient, tmp_path: Path) -> None:
        zip_bytes = build_gtfs_zip()
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(zip_bytes)

        response = await api_client.post(
            "/admin/import/static-gtfs",
            json={
                "source_type": "local",
                "source": str(zip_file),
                "dry_run": True,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["feed_hash"]
        assert data["counts"]["stops"]["read"] == 3
        assert data["counts"]["routes"]["read"] == 2
        assert data["counts"]["trips"]["read"] == 3
        assert data["counts"]["stop_times"]["read"] == 8

    async def test_invalid_body_missing_source_for_local(self, api_client: AsyncClient) -> None:
        response = await api_client.post(
            "/admin/import/static-gtfs",
            json={
                "source_type": "local",
                "source": "",
            },
        )
        assert response.status_code == 400

    async def test_local_source_not_found(self, api_client: AsyncClient) -> None:
        response = await api_client.post(
            "/admin/import/static-gtfs",
            json={
                "source_type": "local",
                "source": "/nonexistent/gtfs.zip",
            },
        )
        assert response.status_code == 400

    async def test_missing_gtfs_file_returns_400(
        self, api_client: AsyncClient, tmp_path: Path
    ) -> None:
        zip_bytes = build_gtfs_zip(exclude_files={"stops.txt"})
        zip_file = tmp_path / "bad_gtfs.zip"
        zip_file.write_bytes(zip_bytes)

        response = await api_client.post(
            "/admin/import/static-gtfs",
            json={
                "source_type": "local",
                "source": str(zip_file),
                "dry_run": True,
            },
        )
        assert response.status_code == 400
        assert "stops.txt" in response.json()["detail"]

    async def test_default_remote_source_uses_config(self, api_client: AsyncClient) -> None:
        """When source is empty with remote type, it should use configured URL."""
        # Mock the importer to avoid actual network call
        mock_report = ImportReport(source="https://gtfs-static.translink.ca", feed_hash="abc123")
        mock_report.init_table("stops")
        mock_report.init_table("routes")
        mock_report.init_table("trips")
        mock_report.init_table("stop_times")
        mock_report.finish()

        with patch(
            "transit_api.routers.admin.GtfsImporter.run",
            new_callable=AsyncMock,
            return_value=mock_report,
        ):
            response = await api_client.post(
                "/admin/import/static-gtfs",
                json={"source_type": "remote", "source": ""},
            )
            assert response.status_code == 200

    async def test_batch_size_validation(self, api_client: AsyncClient) -> None:
        response = await api_client.post(
            "/admin/import/static-gtfs",
            json={
                "source_type": "local",
                "source": "/tmp/test.zip",
                "batch_size": 0,  # below minimum
            },
        )
        assert response.status_code == 422  # Pydantic validation error

    async def test_batch_size_over_max(self, api_client: AsyncClient) -> None:
        response = await api_client.post(
            "/admin/import/static-gtfs",
            json={
                "source_type": "local",
                "source": "/tmp/test.zip",
                "batch_size": 99999,
            },
        )
        assert response.status_code == 422

    async def test_strict_mode_failure_returns_400(
        self, api_client: AsyncClient, tmp_path: Path
    ) -> None:
        bad_stops = (
            "stop_id,stop_name,stop_lat,stop_lon\n50001,Test,abc,def\n50002,Good,49.0,-123.0\n"
        )
        zip_bytes = build_gtfs_zip(stops=bad_stops)
        zip_file = tmp_path / "bad_gtfs.zip"
        zip_file.write_bytes(zip_bytes)

        response = await api_client.post(
            "/admin/import/static-gtfs",
            json={
                "source_type": "local",
                "source": str(zip_file),
                "dry_run": True,
                "strict": True,
            },
        )

        assert response.status_code == 400
        detail = response.json().get("detail", {})
        assert "errors" in detail
        assert detail["status"] == "failed"
