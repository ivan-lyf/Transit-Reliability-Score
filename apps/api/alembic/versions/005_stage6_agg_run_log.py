"""stage6_agg_run_log: add aggregation run log table for Stage 6.

Tracks each aggregation job execution: timing, rows processed, status.
Used by GET /meta/last-agg to surface the most recent run summary.

Revision ID: 005
Revises: 004
Create Date: 2026-02-18 00:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "005"
down_revision: str | Sequence[str] | None = "004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "agg_run_log",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lookback_days", sa.Integer, nullable=False),
        sa.Column("rows_scanned", sa.Integer, nullable=False, server_default="0"),
        sa.Column("buckets_updated", sa.Integer, nullable=False, server_default="0"),
        sa.Column(
            "status", sa.String(32), nullable=False, server_default="running"
        ),
        sa.Column("error_message", sa.Text, nullable=False, server_default=""),
    )
    op.create_index("ix_agg_run_log_started_at", "agg_run_log", ["started_at"])


def downgrade() -> None:
    op.drop_index("ix_agg_run_log_started_at", table_name="agg_run_log")
    op.drop_table("agg_run_log")
