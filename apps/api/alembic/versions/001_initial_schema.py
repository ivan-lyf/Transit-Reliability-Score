"""Initial schema with all tables.

Revision ID: 001
Revises:
Create Date: 2024-01-01 00:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Create stops table
    op.create_table(
        "stops",
        sa.Column("stop_id", sa.String(64), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("lat", sa.Numeric(10, 7), nullable=False),
        sa.Column("lon", sa.Numeric(10, 7), nullable=False),
        sa.PrimaryKeyConstraint("stop_id"),
    )
    op.create_index("ix_stops_lat_lon", "stops", ["lat", "lon"])

    # Create routes table
    op.create_table(
        "routes",
        sa.Column("route_id", sa.String(64), nullable=False),
        sa.Column("short_name", sa.String(64), nullable=False),
        sa.Column("long_name", sa.String(255), nullable=False),
        sa.PrimaryKeyConstraint("route_id"),
    )

    # Create trips table
    op.create_table(
        "trips",
        sa.Column("trip_id", sa.String(128), nullable=False),
        sa.Column("route_id", sa.String(64), nullable=False),
        sa.Column("service_id", sa.String(64), nullable=False),
        sa.Column("direction_id", sa.Integer(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("trip_id"),
        sa.ForeignKeyConstraint(["route_id"], ["routes.route_id"], ondelete="CASCADE"),
    )
    op.create_index("ix_trips_route_id", "trips", ["route_id"])

    # Create stop_times table
    op.create_table(
        "stop_times",
        sa.Column("trip_id", sa.String(128), nullable=False),
        sa.Column("stop_id", sa.String(64), nullable=False),
        sa.Column("stop_sequence", sa.Integer(), nullable=False),
        sa.Column("sched_arrival_sec", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("trip_id", "stop_id", "stop_sequence"),
        sa.ForeignKeyConstraint(["trip_id"], ["trips.trip_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["stop_id"], ["stops.stop_id"], ondelete="CASCADE"),
    )
    op.create_index("ix_stop_times_stop_id", "stop_times", ["stop_id"])
    op.create_index("ix_stop_times_trip_id", "stop_times", ["trip_id"])

    # Create rt_observations table
    op.create_table(
        "rt_observations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("trip_id", sa.String(128), nullable=False),
        sa.Column("stop_id", sa.String(64), nullable=False),
        sa.Column("observed_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("delay_sec", sa.Integer(), nullable=False),
        sa.Column("source_ts", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["trip_id"], ["trips.trip_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["stop_id"], ["stops.stop_id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_rt_observations_stop_observed",
        "rt_observations",
        ["stop_id", "observed_ts"],
    )
    op.create_index(
        "ix_rt_observations_trip_stop_observed",
        "rt_observations",
        ["trip_id", "stop_id", "observed_ts"],
    )

    # Create score_agg table
    op.create_table(
        "score_agg",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("stop_id", sa.String(64), nullable=False),
        sa.Column("route_id", sa.String(64), nullable=False),
        sa.Column("day_type", sa.String(16), nullable=False),
        sa.Column("hour_bucket", sa.String(8), nullable=False),
        sa.Column("on_time_rate", sa.Numeric(5, 4), nullable=False),
        sa.Column("p50_delay_sec", sa.Integer(), nullable=False),
        sa.Column("p95_delay_sec", sa.Integer(), nullable=False),
        sa.Column("score", sa.Integer(), nullable=False),
        sa.Column("sample_n", sa.Integer(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["stop_id"], ["stops.stop_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["route_id"], ["routes.route_id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "stop_id", "route_id", "day_type", "hour_bucket", name="uq_score_agg_key"
        ),
        sa.CheckConstraint("day_type IN ('weekday', 'saturday', 'sunday')", name="ck_day_type"),
        sa.CheckConstraint(
            "hour_bucket IN ('6-9', '9-12', '12-15', '15-18', '18-21')",
            name="ck_hour_bucket",
        ),
        sa.CheckConstraint("score >= 0 AND score <= 100", name="ck_score_range"),
        sa.CheckConstraint("on_time_rate >= 0 AND on_time_rate <= 1", name="ck_on_time_rate"),
        sa.CheckConstraint("sample_n >= 0", name="ck_sample_n"),
    )
    op.create_index(
        "ix_score_agg_lookup",
        "score_agg",
        ["stop_id", "route_id", "day_type", "hour_bucket"],
    )
    op.create_index("ix_score_agg_stop_score", "score_agg", ["stop_id", "score"])

    # Create users table
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("auth_id", sa.String(64), nullable=False),
        sa.Column(
            "favorites_json",
            sa.Text(),
            nullable=False,
            server_default='{"stops": [], "routes": []}',
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("auth_id"),
    )
    op.create_index("ix_users_auth_id", "users", ["auth_id"])


def downgrade() -> None:
    # Drop tables in reverse order (respecting foreign key constraints)
    op.drop_table("users")
    op.drop_table("score_agg")
    op.drop_table("rt_observations")
    op.drop_table("stop_times")
    op.drop_table("trips")
    op.drop_table("routes")
    op.drop_table("stops")
