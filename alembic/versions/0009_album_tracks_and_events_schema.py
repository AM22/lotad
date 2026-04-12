"""Clean up album_tracks and album_events schema

album_tracks: drop youtube_video_id and youtube_timestamp_seconds — these
belong at the playlist_songs level (composite-video problem), not on a
canonical track listing.

album_events: add touhoudb_id so we can store the TouhouDB event ID returned
by the album detail API for future extensibility.

Revision ID: 0009
Revises: 0008
Create Date: 2026-04-11
"""

import sqlalchemy as sa
from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("album_tracks_youtube_video_id_fkey", "album_tracks", type_="foreignkey")
    op.drop_column("album_tracks", "youtube_video_id")
    op.drop_column("album_tracks", "youtube_timestamp_seconds")
    op.add_column("album_events", sa.Column("touhoudb_id", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("album_events", "touhoudb_id")
    op.add_column(
        "album_tracks",
        sa.Column("youtube_timestamp_seconds", sa.Integer(), nullable=True),
    )
    op.add_column(
        "album_tracks",
        sa.Column(
            "youtube_video_id",
            sa.Integer(),
            sa.ForeignKey("youtube_videos.id"),
            nullable=True,
        ),
    )
