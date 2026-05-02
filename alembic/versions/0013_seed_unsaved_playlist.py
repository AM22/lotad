"""Seed the unsaved playlist (id 6) and add it to all scoring configurations

The `unsaved` playlist is the M7/M8 anchor for "listened but consciously not
kept". M7 sync moves dropped playlist 3 / eval entries here silently; future
M8 work will populate it from album cross-references and watch history.

Weight is 0 in every scoring config (same as eval).

Revision ID: 0013
Revises: 0012
Create Date: 2026-05-02
"""

from alembic import op

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None

_PLAYLIST_NAME = "unsaved"
_PLAYLIST_YOUTUBE_ID = "__lotad_unsaved__"
_DISPLAY_ORDER = 6


def upgrade() -> None:
    op.execute(
        f"""
        INSERT INTO playlists (name, youtube_playlist_id, display_order)
        VALUES ('{_PLAYLIST_NAME}', '{_PLAYLIST_YOUTUBE_ID}', {_DISPLAY_ORDER})
        ON CONFLICT (youtube_playlist_id) DO NOTHING
        """
    )

    # Add "unsaved": 0 to every scoring_configurations.weights JSON.
    # The column is plain json (not jsonb), so cast for the merge operator.
    op.execute(
        """
        UPDATE scoring_configurations
        SET weights = (weights::jsonb || '{"unsaved": 0}'::jsonb)::json
        WHERE NOT (weights::jsonb ? 'unsaved')
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE scoring_configurations
        SET weights = (weights::jsonb - 'unsaved')::json
        WHERE weights::jsonb ? 'unsaved'
        """
    )
    op.execute(f"DELETE FROM playlists WHERE youtube_playlist_id = '{_PLAYLIST_YOUTUBE_ID}'")
