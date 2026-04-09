"""Add touhoudb_id to characters table.

Enables upsert-by-id when ingesting character artists from TouhouDB API
responses.  Characters are credited on songs as "subjects" (e.g. Yoshika
Miyako, Seiga) and their TouhouDB artist IDs are used to populate the
song_characters join table during ingestion.

Revision ID: 0003
Revises: 0002
Create Date: 2026-04-08
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision: str = "0003"
down_revision: str = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "characters",
        sa.Column("touhoudb_id", sa.Integer, nullable=True),
    )
    op.create_index(
        "ix_characters_touhoudb_id",
        "characters",
        ["touhoudb_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_characters_touhoudb_id", table_name="characters")
    op.drop_column("characters", "touhoudb_id")
