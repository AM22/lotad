"""Make original_songs.work_id nullable

Original songs from non-Touhou ZUN works (Seihou games, unreleased tracks,
etc.) may not have a corresponding work in the works table.  These songs
still need to exist in original_songs so that arrangements can be linked to
them via song_originals — a missing work_id should not block task resolution.

Revision ID: 0008
Revises: 0007
Create Date: 2026-04-11
"""

import sqlalchemy as sa

from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("original_songs", "work_id", existing_type=sa.Integer(), nullable=True)


def downgrade() -> None:
    # Re-adding NOT NULL requires all rows to have a value — safe only if no
    # NULL work_ids exist at downgrade time.
    op.alter_column("original_songs", "work_id", existing_type=sa.Integer(), nullable=False)
