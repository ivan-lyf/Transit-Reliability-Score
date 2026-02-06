"""Tests for database migrations."""

import importlib.util
import sys
from pathlib import Path

import pytest


class TestMigrationScript:
    """Tests for migration script structure."""

    @pytest.fixture
    def migration_module(self) -> object:
        """Load the initial migration module."""
        migration_path = Path(__file__).parent.parent / "alembic/versions/001_initial_schema.py"
        spec = importlib.util.spec_from_file_location("migration_001", migration_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules["migration_001"] = module
        spec.loader.exec_module(module)
        return module

    def test_migration_has_revision_id(self, migration_module: object) -> None:
        """Verify migration has a revision identifier."""
        assert hasattr(migration_module, "revision")
        assert migration_module.revision == "001"  # type: ignore[attr-defined]

    def test_migration_has_down_revision(self, migration_module: object) -> None:
        """Verify migration has down_revision set."""
        assert hasattr(migration_module, "down_revision")
        assert migration_module.down_revision is None  # type: ignore[attr-defined]

    def test_migration_has_upgrade_function(self, migration_module: object) -> None:
        """Verify migration has upgrade function."""
        assert hasattr(migration_module, "upgrade")
        assert callable(migration_module.upgrade)  # type: ignore[attr-defined]

    def test_migration_has_downgrade_function(self, migration_module: object) -> None:
        """Verify migration has downgrade function."""
        assert hasattr(migration_module, "downgrade")
        assert callable(migration_module.downgrade)  # type: ignore[attr-defined]


class TestMigrationUpgradeOperations:
    """Tests for verifying the upgrade creates correct structures."""

    @pytest.fixture
    def migration_source(self) -> str:
        """Load migration source code for inspection."""
        migration_path = Path(__file__).parent.parent / "alembic/versions/001_initial_schema.py"
        return migration_path.read_text()

    def test_creates_stops_table(self, migration_source: str) -> None:
        """Verify upgrade creates stops table."""
        assert 'op.create_table(\n        "stops"' in migration_source

    def test_creates_routes_table(self, migration_source: str) -> None:
        """Verify upgrade creates routes table."""
        assert 'op.create_table(\n        "routes"' in migration_source

    def test_creates_trips_table(self, migration_source: str) -> None:
        """Verify upgrade creates trips table."""
        assert 'op.create_table(\n        "trips"' in migration_source

    def test_creates_stop_times_table(self, migration_source: str) -> None:
        """Verify upgrade creates stop_times table."""
        assert 'op.create_table(\n        "stop_times"' in migration_source

    def test_creates_rt_observations_table(self, migration_source: str) -> None:
        """Verify upgrade creates rt_observations table."""
        assert 'op.create_table(\n        "rt_observations"' in migration_source

    def test_creates_score_agg_table(self, migration_source: str) -> None:
        """Verify upgrade creates score_agg table."""
        assert 'op.create_table(\n        "score_agg"' in migration_source

    def test_creates_users_table(self, migration_source: str) -> None:
        """Verify upgrade creates users table."""
        assert 'op.create_table(\n        "users"' in migration_source

    def test_creates_spatial_index(self, migration_source: str) -> None:
        """Verify upgrade creates spatial index on stops."""
        assert '"ix_stops_lat_lon"' in migration_source

    def test_creates_score_lookup_index(self, migration_source: str) -> None:
        """Verify upgrade creates score lookup index."""
        assert '"ix_score_agg_lookup"' in migration_source

    def test_creates_observations_index(self, migration_source: str) -> None:
        """Verify upgrade creates observations index."""
        assert '"ix_rt_observations_stop_observed"' in migration_source


class TestMigrationDowngradeOperations:
    """Tests for verifying the downgrade removes all structures."""

    @pytest.fixture
    def migration_source(self) -> str:
        """Load migration source code for inspection."""
        migration_path = Path(__file__).parent.parent / "alembic/versions/001_initial_schema.py"
        return migration_path.read_text()

    def test_downgrade_drops_all_tables(self, migration_source: str) -> None:
        """Verify downgrade drops all tables."""
        downgrade_section = migration_source.split("def downgrade")[1]
        assert 'op.drop_table("users")' in downgrade_section
        assert 'op.drop_table("score_agg")' in downgrade_section
        assert 'op.drop_table("rt_observations")' in downgrade_section
        assert 'op.drop_table("stop_times")' in downgrade_section
        assert 'op.drop_table("trips")' in downgrade_section
        assert 'op.drop_table("routes")' in downgrade_section
        assert 'op.drop_table("stops")' in downgrade_section

    def test_downgrade_respects_fk_order(self, migration_source: str) -> None:
        """Verify downgrade drops tables in correct order (child tables first)."""
        downgrade_section = migration_source.split("def downgrade")[1]

        # Find positions of drop statements
        _users_pos = downgrade_section.find('op.drop_table("users")')
        score_agg_pos = downgrade_section.find('op.drop_table("score_agg")')
        rt_obs_pos = downgrade_section.find('op.drop_table("rt_observations")')
        stop_times_pos = downgrade_section.find('op.drop_table("stop_times")')
        trips_pos = downgrade_section.find('op.drop_table("trips")')
        routes_pos = downgrade_section.find('op.drop_table("routes")')
        stops_pos = downgrade_section.find('op.drop_table("stops")')

        # Child tables must be dropped before parent tables
        assert stop_times_pos < trips_pos  # stop_times refs trips
        assert stop_times_pos < stops_pos  # stop_times refs stops
        assert rt_obs_pos < trips_pos  # rt_observations refs trips
        assert rt_obs_pos < stops_pos  # rt_observations refs stops
        assert trips_pos < routes_pos  # trips refs routes
        assert score_agg_pos < stops_pos  # score_agg refs stops
        assert score_agg_pos < routes_pos  # score_agg refs routes
