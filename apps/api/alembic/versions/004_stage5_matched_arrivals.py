"""stage5_matched_arrivals: add matched arrivals table for schedule-to-observation matching.

Creates the matched_arrivals table for storing the result of matching
GTFS-RT trip updates to static scheduled stop times, with computed delay.

Revision ID: 004
Revises: 003
Create Date: 2026-02-06 18:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "004"
down_revision: str | Sequence[str] | None = "003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "matched_arrivals",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("trip_id", sa.String(128), nullable=False),
        sa.Column("stop_id", sa.String(64), nullable=False),
        sa.Column("stop_sequence", sa.Integer(), nullable=False),
        sa.Column("service_date", sa.Date(), nullable=False),
        sa.Column("scheduled_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("delay_sec", sa.Integer(), nullable=False),
        sa.Column(
            "match_status",
            sa.String(16),
            nullable=False,
            server_default="matched",
        ),
        sa.Column(
            "match_confidence",
            sa.Float(),
            nullable=False,
            server_default="1.0",
        ),
        sa.Column("source_feed_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("rt_trip_update_id", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # Unique constraint for idempotency: one result per scheduled stop per service day
    op.create_index(
        "uq_matched_arrival_key",
        "matched_arrivals",
        ["trip_id", "stop_id", "stop_sequence", "service_date"],
        unique=True,
    )

    # Lookup indexes
    op.create_index(
        "ix_matched_trip_stop_date",
        "matched_arrivals",
        ["trip_id", "stop_id", "service_date"],
    )
    op.create_index(
        "ix_matched_stop_observed",
        "matched_arrivals",
        ["stop_id", "observed_ts"],
    )
    op.create_index(
        "ix_matched_date_trip",
        "matched_arrivals",
        ["service_date", "trip_id"],
    )


def downgrade() -> None:
    op.drop_table("matched_arrivals")
