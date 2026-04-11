"""Rework original_songs schema: drop is_extra_stage, track_number; move BPM to original_songs

Stage is now encoded as an integer (0=title, 1-6=stage N, 7=extra, 8=ending, 9=staff roll).
is_extra_stage is folded into stage=7.
track_number is not derivable from TouhouDB and is not needed.
min_milli_bpm and max_milli_bpm are removed from songs (never populated there) and added
to original_songs (BPM is a property of ZUN's original compositions, not arrangements).

Revision ID: 0006
Revises: 0005
Create Date: 2026-04-11
"""

import sqlalchemy as sa
from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("original_songs", "is_extra_stage")
    op.drop_column("original_songs", "track_number")
    op.drop_column("songs", "min_milli_bpm")
    op.drop_column("songs", "max_milli_bpm")
    op.add_column("original_songs", sa.Column("min_milli_bpm", sa.Integer(), nullable=True))
    op.add_column("original_songs", sa.Column("max_milli_bpm", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("original_songs", "max_milli_bpm")
    op.drop_column("original_songs", "min_milli_bpm")
    op.add_column("songs", sa.Column("max_milli_bpm", sa.Integer(), nullable=True))
    op.add_column("songs", sa.Column("min_milli_bpm", sa.Integer(), nullable=True))
    op.add_column("original_songs", sa.Column("track_number", sa.Integer(), nullable=True))
    op.add_column(
        "original_songs",
        sa.Column("is_extra_stage", sa.Boolean(), server_default="false", nullable=False),
    )
