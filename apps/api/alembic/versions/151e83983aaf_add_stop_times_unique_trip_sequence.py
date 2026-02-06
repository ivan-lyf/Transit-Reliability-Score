"""add stop_times unique trip sequence

Revision ID: 151e83983aaf
Revises: 001
Create Date: 2026-02-05 00:08:04.603612

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "151e83983aaf"
down_revision: str | Sequence[str] | None = "001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_unique_constraint(
        "uq_stop_times_trip_sequence",
        "stop_times",
        ["trip_id", "stop_sequence"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        "uq_stop_times_trip_sequence",
        "stop_times",
        type_="unique",
    )
