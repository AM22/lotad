"""Scope dropped-upload tasks to their source playlist.

Revision ID: 0015
Revises: 0014
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("uq_tasks_open_video", table_name="tasks")
    op.drop_index("uq_tasks_open_song", table_name="tasks")
    for suffix, column in (("video", "related_video_id"), ("song", "related_song_id")):
        op.create_index(
            f"uq_tasks_open_{suffix}",
            "tasks",
            ["task_type", column],
            unique=True,
            postgresql_where=sa.text(
                f"status = 'OPEN' AND {column} IS NOT NULL AND task_type <> 'DROPPED_VIDEO'"
            ),
        )
    # Legacy ingestion payloads used playlist_db_id. The prior video index
    # already prevents duplicates when upgrading existing OPEN tasks.
    op.create_index(
        "uq_tasks_open_dropped_playlist",
        "tasks",
        [
            "related_video_id",
            sa.text("coalesce(data->>'source_playlist_db_id', data->>'playlist_db_id', '')"),
        ],
        unique=True,
        postgresql_where=sa.text(
            "status = 'OPEN' AND related_video_id IS NOT NULL AND task_type = 'DROPPED_VIDEO'"
        ),
    )


def downgrade() -> None:
    # Preserve review history instead of deleting tasks to satisfy the old
    # global indexes. The caller can resolve duplicates before downgrading.
    conflict = (
        op.get_bind()
        .execute(
            sa.text("""
        SELECT 1 FROM tasks
        WHERE status = 'OPEN' AND related_video_id IS NOT NULL
        GROUP BY task_type, related_video_id HAVING count(*) > 1
        UNION ALL
        SELECT 1 FROM tasks
        WHERE status = 'OPEN' AND related_song_id IS NOT NULL
        GROUP BY task_type, related_song_id HAVING count(*) > 1
        LIMIT 1
    """)
        )
        .first()
    )
    if conflict is not None:
        raise RuntimeError(
            "Resolve or dismiss duplicate OPEN tasks for each video/song before downgrading 0015."
        )
    op.drop_index("uq_tasks_open_dropped_playlist", table_name="tasks")
    for suffix, column in (("video", "related_video_id"), ("song", "related_song_id")):
        op.drop_index(f"uq_tasks_open_{suffix}", table_name="tasks")
        op.create_index(
            f"uq_tasks_open_{suffix}",
            "tasks",
            ["task_type", column],
            unique=True,
            postgresql_where=sa.text(f"status = 'OPEN' AND {column} IS NOT NULL"),
        )
