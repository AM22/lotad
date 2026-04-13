"""Add llm_enriched_at to tasks table

Tracks when a task was last processed by the LLM enrichment pipeline.
Allows efficient querying for unenriched INGEST_FAILED tasks without
scanning the data JSON column.

Revision ID: 0011
Revises: 0010
Create Date: 2026-04-13
"""

import sqlalchemy as sa
from alembic import op

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tasks", sa.Column("llm_enriched_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("tasks", "llm_enriched_at")
