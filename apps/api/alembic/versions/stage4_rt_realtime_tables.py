"""stage4_realtime_tables: add GTFS-RT ingestion tables.

Creates tables for storing normalized GTFS-Realtime data:
- rt_trip_updates: Trip delay/schedule updates per stop
- rt_vehicle_positions: Vehicle GPS positions
- rt_alerts: Service alerts with informed entities
- rt_ingest_meta: Feed ingestion status tracking

Revision ID: 003
Revises: 002
Create Date: 2026-02-06 12:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "003"
down_revision: str | Sequence[str] | None = "002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # --- rt_trip_updates ---
    op.create_table(
        "rt_trip_updates",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("trip_id", sa.String(128), nullable=False),
        sa.Column("route_id", sa.String(64), nullable=False, server_default=""),
        sa.Column("stop_id", sa.String(64), nullable=False),
        sa.Column("stop_sequence", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("arrival_delay", sa.Integer(), nullable=True),
        sa.Column("arrival_time", sa.Integer(), nullable=True),
        sa.Column("departure_delay", sa.Integer(), nullable=True),
        sa.Column("departure_time", sa.Integer(), nullable=True),
        sa.Column(
            "schedule_relationship",
            sa.String(32),
            nullable=False,
            server_default="SCHEDULED",
        ),
        sa.Column("feed_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_rt_trip_updates_trip_id", "rt_trip_updates", ["trip_id"])
    op.create_index("ix_rt_trip_updates_stop_id", "rt_trip_updates", ["stop_id"])
    op.create_index("ix_rt_trip_updates_feed_ts", "rt_trip_updates", ["feed_timestamp"])
    op.create_index(
        "ix_rt_trip_updates_dedup",
        "rt_trip_updates",
        ["trip_id", "stop_id", "feed_timestamp"],
        unique=True,
    )

    # --- rt_vehicle_positions ---
    op.create_table(
        "rt_vehicle_positions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("vehicle_id", sa.String(64), nullable=False),
        sa.Column("trip_id", sa.String(128), nullable=False, server_default=""),
        sa.Column("route_id", sa.String(64), nullable=False, server_default=""),
        sa.Column("latitude", sa.Float(), nullable=False),
        sa.Column("longitude", sa.Float(), nullable=False),
        sa.Column("bearing", sa.Float(), nullable=True),
        sa.Column("speed", sa.Float(), nullable=True),
        sa.Column("current_stop_sequence", sa.Integer(), nullable=True),
        sa.Column("current_status", sa.String(32), nullable=False, server_default=""),
        sa.Column("feed_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_rt_vehicle_pos_vehicle_id", "rt_vehicle_positions", ["vehicle_id"]
    )
    op.create_index("ix_rt_vehicle_pos_trip_id", "rt_vehicle_positions", ["trip_id"])
    op.create_index("ix_rt_vehicle_pos_route_id", "rt_vehicle_positions", ["route_id"])
    op.create_index(
        "ix_rt_vehicle_pos_feed_ts", "rt_vehicle_positions", ["feed_timestamp"]
    )
    op.create_index(
        "ix_rt_vehicle_pos_dedup",
        "rt_vehicle_positions",
        ["vehicle_id", "feed_timestamp"],
        unique=True,
    )

    # --- rt_alerts ---
    op.create_table(
        "rt_alerts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("alert_id", sa.String(128), nullable=False),
        sa.Column(
            "cause", sa.String(64), nullable=False, server_default="UNKNOWN_CAUSE"
        ),
        sa.Column(
            "effect", sa.String(64), nullable=False, server_default="UNKNOWN_EFFECT"
        ),
        sa.Column("header_text", sa.Text(), nullable=False, server_default=""),
        sa.Column("description_text", sa.Text(), nullable=False, server_default=""),
        sa.Column("active_period_start", sa.Integer(), nullable=True),
        sa.Column("active_period_end", sa.Integer(), nullable=True),
        sa.Column("informed_route_id", sa.String(64), nullable=False, server_default=""),
        sa.Column("informed_stop_id", sa.String(64), nullable=False, server_default=""),
        sa.Column("informed_trip_id", sa.String(128), nullable=False, server_default=""),
        sa.Column("feed_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_rt_alerts_alert_id", "rt_alerts", ["alert_id"])
    op.create_index("ix_rt_alerts_route_id", "rt_alerts", ["informed_route_id"])
    op.create_index("ix_rt_alerts_feed_ts", "rt_alerts", ["feed_timestamp"])
    op.create_index(
        "ix_rt_alerts_dedup",
        "rt_alerts",
        ["alert_id", "informed_route_id", "informed_stop_id", "feed_timestamp"],
        unique=True,
    )

    # --- rt_ingest_meta ---
    op.create_table(
        "rt_ingest_meta",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("feed_type", sa.String(32), nullable=False, unique=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(32), nullable=False, server_default="unknown"),
        sa.Column("error_message", sa.Text(), nullable=False, server_default=""),
        sa.Column("feed_hash", sa.String(64), nullable=False, server_default=""),
        sa.Column("entity_count", sa.Integer(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_rt_ingest_meta_feed_type", "rt_ingest_meta", ["feed_type"])


def downgrade() -> None:
    op.drop_table("rt_ingest_meta")
    op.drop_table("rt_alerts")
    op.drop_table("rt_vehicle_positions")
    op.drop_table("rt_trip_updates")
