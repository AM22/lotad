"""Track original links that must survive upstream metadata refresh.

Revision ID: 0014
Revises: 0013
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "song_originals",
        sa.Column("is_manual", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    # The existing application writes non-upstream links only for unmatched stubs.
    op.execute(
        """
        UPDATE song_originals SET is_manual = TRUE
        FROM songs
        WHERE songs.id = song_originals.song_id AND songs.touhoudb_id IS NULL
        """
    )


def downgrade() -> None:
    op.drop_column("song_originals", "is_manual")
