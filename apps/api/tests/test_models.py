"""Tests for SQLAlchemy models."""

from transit_api.models import (
    Base,
    User,
)


class TestModelsMetadata:
    """Tests for model metadata and table structure."""

    def test_all_tables_registered(self) -> None:
        """Verify all expected tables are in the metadata."""
        expected_tables = {
            "stops",
            "routes",
            "trips",
            "stop_times",
            "rt_observations",
            "rt_trip_updates",
            "rt_vehicle_positions",
            "rt_alerts",
            "rt_ingest_meta",
            "matched_arrivals",
            "score_agg",
            "users",
            "gtfs_import_log",
        }
        actual_tables = set(Base.metadata.tables.keys())
        assert expected_tables == actual_tables

    def test_stops_table_columns(self) -> None:
        """Verify stops table has correct columns."""
        table = Base.metadata.tables["stops"]
        columns = {c.name for c in table.columns}
        assert columns == {"stop_id", "name", "lat", "lon"}

    def test_routes_table_columns(self) -> None:
        """Verify routes table has correct columns."""
        table = Base.metadata.tables["routes"]
        columns = {c.name for c in table.columns}
        assert columns == {"route_id", "short_name", "long_name"}

    def test_trips_table_columns(self) -> None:
        """Verify trips table has correct columns."""
        table = Base.metadata.tables["trips"]
        columns = {c.name for c in table.columns}
        assert columns == {"trip_id", "route_id", "service_id", "direction_id"}

    def test_stop_times_table_columns(self) -> None:
        """Verify stop_times table has correct columns."""
        table = Base.metadata.tables["stop_times"]
        columns = {c.name for c in table.columns}
        assert columns == {"trip_id", "stop_id", "stop_sequence", "sched_arrival_sec"}

    def test_rt_observations_table_columns(self) -> None:
        """Verify rt_observations table has correct columns."""
        table = Base.metadata.tables["rt_observations"]
        columns = {c.name for c in table.columns}
        assert columns == {"id", "trip_id", "stop_id", "observed_ts", "delay_sec", "source_ts"}

    def test_score_agg_table_columns(self) -> None:
        """Verify score_agg table has correct columns."""
        table = Base.metadata.tables["score_agg"]
        columns = {c.name for c in table.columns}
        expected = {
            "id",
            "stop_id",
            "route_id",
            "day_type",
            "hour_bucket",
            "on_time_rate",
            "p50_delay_sec",
            "p95_delay_sec",
            "score",
            "sample_n",
            "updated_at",
        }
        assert columns == expected

    def test_users_table_columns(self) -> None:
        """Verify users table has correct columns."""
        table = Base.metadata.tables["users"]
        columns = {c.name for c in table.columns}
        assert columns == {"id", "auth_id", "favorites_json", "created_at", "updated_at"}


class TestStopsTableIndexes:
    """Tests for stops table indexes."""

    def test_stops_has_lat_lon_index(self) -> None:
        """Verify stops table has spatial index on lat/lon."""
        table = Base.metadata.tables["stops"]
        index_names = {idx.name for idx in table.indexes}
        assert "ix_stops_lat_lon" in index_names


class TestScoreAggIndexes:
    """Tests for score_agg table indexes."""

    def test_score_agg_has_lookup_index(self) -> None:
        """Verify score_agg has the composite lookup index."""
        table = Base.metadata.tables["score_agg"]
        index_names = {idx.name for idx in table.indexes}
        assert "ix_score_agg_lookup" in index_names

    def test_score_agg_lookup_index_columns(self) -> None:
        """Verify lookup index includes correct columns."""
        table = Base.metadata.tables["score_agg"]
        lookup_idx = next(idx for idx in table.indexes if idx.name == "ix_score_agg_lookup")
        idx_columns = [c.name for c in lookup_idx.columns]
        assert idx_columns == ["stop_id", "route_id", "day_type", "hour_bucket"]


class TestRtObservationsIndexes:
    """Tests for rt_observations table indexes."""

    def test_rt_observations_has_stop_observed_index(self) -> None:
        """Verify rt_observations has index for stop + observed_ts queries."""
        table = Base.metadata.tables["rt_observations"]
        index_names = {idx.name for idx in table.indexes}
        assert "ix_rt_observations_stop_observed" in index_names


class TestScoreAggConstraints:
    """Tests for score_agg table constraints."""

    def test_score_agg_has_unique_constraint(self) -> None:
        """Verify score_agg has unique constraint on aggregation key."""
        table = Base.metadata.tables["score_agg"]
        constraint_names = {c.name for c in table.constraints if c.name}
        assert "uq_score_agg_key" in constraint_names

    def test_score_agg_has_check_constraints(self) -> None:
        """Verify score_agg has check constraints."""
        table = Base.metadata.tables["score_agg"]
        constraint_names = {c.name for c in table.constraints if c.name}
        assert "ck_day_type" in constraint_names
        assert "ck_hour_bucket" in constraint_names
        assert "ck_score_range" in constraint_names
        assert "ck_on_time_rate" in constraint_names
        assert "ck_sample_n" in constraint_names


class TestForeignKeys:
    """Tests for foreign key relationships."""

    def test_trips_references_routes(self) -> None:
        """Verify trips.route_id references routes.route_id."""
        table = Base.metadata.tables["trips"]
        fks = list(table.foreign_keys)
        fk_targets = {str(fk.target_fullname) for fk in fks}
        assert "routes.route_id" in fk_targets

    def test_stop_times_references_trips_and_stops(self) -> None:
        """Verify stop_times references both trips and stops."""
        table = Base.metadata.tables["stop_times"]
        fks = list(table.foreign_keys)
        fk_targets = {str(fk.target_fullname) for fk in fks}
        assert "trips.trip_id" in fk_targets
        assert "stops.stop_id" in fk_targets

    def test_rt_observations_references_trips_and_stops(self) -> None:
        """Verify rt_observations references both trips and stops."""
        table = Base.metadata.tables["rt_observations"]
        fks = list(table.foreign_keys)
        fk_targets = {str(fk.target_fullname) for fk in fks}
        assert "trips.trip_id" in fk_targets
        assert "stops.stop_id" in fk_targets

    def test_score_agg_references_stops_and_routes(self) -> None:
        """Verify score_agg references both stops and routes."""
        table = Base.metadata.tables["score_agg"]
        fks = list(table.foreign_keys)
        fk_targets = {str(fk.target_fullname) for fk in fks}
        assert "stops.stop_id" in fk_targets
        assert "routes.route_id" in fk_targets


class TestUserModel:
    """Tests for User model methods."""

    def test_user_get_favorites_default(self) -> None:
        """Test default favorites parsing."""
        user = User(auth_id="test123", favorites_json='{"stops": [], "routes": []}')
        favorites = user.get_favorites()
        assert favorites == {"stops": [], "routes": []}

    def test_user_set_favorites(self) -> None:
        """Test favorites serialization."""
        user = User(auth_id="test123")
        user.set_favorites({"stops": ["stop1", "stop2"], "routes": ["route1"]})
        favorites = user.get_favorites()
        assert favorites == {"stops": ["stop1", "stop2"], "routes": ["route1"]}
