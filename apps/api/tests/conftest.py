"""Pytest configuration and fixtures."""

from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from transit_api.main import app


@pytest.fixture
def mock_db_connection() -> Any:
    """Mock database connection check."""
    with patch("transit_api.main.check_database_connection", new_callable=AsyncMock) as mock:
        mock.return_value = True
        yield mock


@pytest.fixture
async def client(mock_db_connection: Any) -> AsyncGenerator[AsyncClient, None]:  # noqa: ARG001
    """Async HTTP client for testing."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
async def client_no_db() -> AsyncGenerator[AsyncClient, None]:
    """Async HTTP client without mocked database (for integration tests)."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
