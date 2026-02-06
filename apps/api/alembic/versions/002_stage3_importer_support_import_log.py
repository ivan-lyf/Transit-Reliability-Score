"""stage3_importer_support: add gtfs_import_log table.

Rationale: The GTFS static importer needs to track feed hashes
to support skip-if-unchanged optimization and import audit trail.
This is an isolated addition that does not modify any Stage 2 tables.

Revision ID: 002
Revises: 151e83983aaf
Create Date: 2026-02-05 12:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "002"
down_revision: str | Sequence[str] | None = "151e83983aaf"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "gtfs_import_log",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("feed_hash", sa.String(64), nullable=False),
        sa.Column(
            "imported_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_gtfs_import_log_imported_at", "gtfs_import_log", ["imported_at"])


def downgrade() -> None:
    op.drop_table("gtfs_import_log")
