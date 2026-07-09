"""add project_id column to events

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-08

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "events",
        sa.Column("project_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_index("ix_events_project_id", "events", ["project_id"])


def downgrade() -> None:
    op.drop_index("ix_events_project_id", table_name="events")
    op.drop_column("events", "project_id")