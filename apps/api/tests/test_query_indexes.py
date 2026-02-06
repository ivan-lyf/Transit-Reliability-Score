"""Tests verifying query patterns will use indexes.

These tests document the critical queries and verify that appropriate
indexes exist to support them. Actual EXPLAIN ANALYZE tests require
a running database and are performed during integration testing.
"""

from transit_api.models import Base


class TestCriticalQueryIndexes:
    """Verify indexes exist for critical query patterns."""

    def test_nearby_stops_query_has_index(self) -> None:
        """
        Critical Query 1: Find stops near a location.

        SELECT * FROM stops
        WHERE lat BETWEEN :min_lat AND :max_lat
          AND lon BETWEEN :min_lon AND :max_lon

        Expected: Uses ix_stops_lat_lon (btree on lat, lon).
        """
        table = Base.metadata.tables["stops"]
        index_names = {idx.name for idx in table.indexes}
        assert "ix_stops_lat_lon" in index_names

        # Verify index columns
        idx = next(i for i in table.indexes if i.name == "ix_stops_lat_lon")
        columns = [c.name for c in idx.columns]
        assert "lat" in columns
        assert "lon" in columns

    def test_score_lookup_query_has_index(self) -> None:
        """
        Critical Query 2: Get score for a stop/route/day/hour.

        SELECT * FROM score_agg
        WHERE stop_id = :stop_id
          AND route_id = :route_id
          AND day_type = :day_type
          AND hour_bucket = :hour_bucket

        Expected: Uses ix_score_agg_lookup (btree on all 4 columns).
        """
        table = Base.metadata.tables["score_agg"]
        index_names = {idx.name for idx in table.indexes}
        assert "ix_score_agg_lookup" in index_names

        # Verify index has all 4 columns in correct order
        idx = next(i for i in table.indexes if i.name == "ix_score_agg_lookup")
        columns = [c.name for c in idx.columns]
        assert columns == ["stop_id", "route_id", "day_type", "hour_bucket"]

    def test_risky_stops_query_has_indexes(self) -> None:
        """
        Critical Query 3: Find risky stops near location.

        SELECT s.*, sa.score, sa.route_id
        FROM stops s
        JOIN score_agg sa ON s.stop_id = sa.stop_id
        WHERE s.lat BETWEEN :min_lat AND :max_lat
          AND s.lon BETWEEN :min_lon AND :max_lon
        ORDER BY sa.score ASC
        LIMIT :limit

        Expected: Uses ix_stops_lat_lon and ix_score_agg_stop_score.
        """
        # Check stops index
        stops_table = Base.metadata.tables["stops"]
        stops_indexes = {idx.name for idx in stops_table.indexes}
        assert "ix_stops_lat_lon" in stops_indexes

        # Check score_agg indexes
        score_table = Base.metadata.tables["score_agg"]
        score_indexes = {idx.name for idx in score_table.indexes}
        assert "ix_score_agg_stop_score" in score_indexes

        # Verify score index supports filtering by stop and ordering by score
        idx = next(i for i in score_table.indexes if i.name == "ix_score_agg_stop_score")
        columns = [c.name for c in idx.columns]
        assert columns == ["stop_id", "score"]

    def test_observations_by_stop_time_has_index(self) -> None:
        """
        Query: Get observations for a stop in time range.

        SELECT * FROM rt_observations
        WHERE stop_id = :stop_id
          AND observed_ts >= :start_ts
          AND observed_ts < :end_ts
        ORDER BY observed_ts DESC

        Expected: Uses ix_rt_observations_stop_observed.
        """
        table = Base.metadata.tables["rt_observations"]
        index_names = {idx.name for idx in table.indexes}
        assert "ix_rt_observations_stop_observed" in index_names

        # Verify index columns
        idx = next(i for i in table.indexes if i.name == "ix_rt_observations_stop_observed")
        columns = [c.name for c in idx.columns]
        assert columns == ["stop_id", "observed_ts"]

    def test_stop_times_by_stop_has_index(self) -> None:
        """
        Query: Get all stop times for a given stop.

        SELECT * FROM stop_times WHERE stop_id = :stop_id

        Expected: Uses ix_stop_times_stop_id.
        """
        table = Base.metadata.tables["stop_times"]
        index_names = {idx.name for idx in table.indexes}
        assert "ix_stop_times_stop_id" in index_names

    def test_trips_by_route_has_index(self) -> None:
        """
        Query: Get all trips for a route.

        SELECT * FROM trips WHERE route_id = :route_id

        Expected: Uses ix_trips_route_id.
        """
        table = Base.metadata.tables["trips"]
        index_names = {idx.name for idx in table.indexes}
        assert "ix_trips_route_id" in index_names

    def test_users_by_auth_id_has_index(self) -> None:
        """
        Query: Find user by auth_id.

        SELECT * FROM users WHERE auth_id = :auth_id

        Expected: Uses ix_users_auth_id (unique).
        """
        table = Base.metadata.tables["users"]
        index_names = {idx.name for idx in table.indexes}
        assert "ix_users_auth_id" in index_names


class TestIndexDocumentation:
    """Document all indexes for reference."""

    def test_list_all_indexes(self) -> None:
        """Document all indexes in the schema."""
        all_indexes: dict[str, list[str]] = {}

        for table_name, table in Base.metadata.tables.items():
            indexes = []
            for idx in table.indexes:
                cols = ", ".join(c.name for c in idx.columns)
                indexes.append(f"{idx.name}({cols})")
            if indexes:
                all_indexes[table_name] = indexes

        # Verify expected tables have indexes
        assert "stops" in all_indexes
        assert "score_agg" in all_indexes
        assert "rt_observations" in all_indexes
        assert "stop_times" in all_indexes
        assert "trips" in all_indexes
        assert "users" in all_indexes

        # This test also serves as documentation when run with -v
        print("\n=== Index Documentation ===")
        for table, indexes in sorted(all_indexes.items()):
            print(f"\n{table}:")
            for idx in indexes:
                print(f"  - {idx}")
