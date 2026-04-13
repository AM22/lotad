"""Deduplicate open tasks and add unique constraints

Delete duplicate OPEN tasks (keeping lowest id per group), then add partial
unique indexes to prevent future duplicates.

Revision ID: 0010
Revises: 0009
Create Date: 2026-04-12
"""

from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Delete duplicate OPEN tasks linked to a video, keeping the oldest (lowest id)
    op.execute("""
        DELETE FROM tasks
        WHERE status = 'OPEN'::task_status
          AND related_video_id IS NOT NULL
          AND id NOT IN (
              SELECT MIN(id)
              FROM tasks
              WHERE status = 'OPEN'::task_status
                AND related_video_id IS NOT NULL
              GROUP BY task_type, related_video_id
          )
    """)

    # Delete duplicate OPEN tasks linked to a song, keeping the oldest (lowest id)
    op.execute("""
        DELETE FROM tasks
        WHERE status = 'OPEN'::task_status
          AND related_song_id IS NOT NULL
          AND id NOT IN (
              SELECT MIN(id)
              FROM tasks
              WHERE status = 'OPEN'::task_status
                AND related_song_id IS NOT NULL
              GROUP BY task_type, related_song_id
          )
    """)

    # Partial unique index: one OPEN task per (task_type, related_video_id)
    op.create_index(
        "uq_tasks_open_video",
        "tasks",
        ["task_type", "related_video_id"],
        unique=True,
        postgresql_where="status = 'OPEN'::task_status AND related_video_id IS NOT NULL",
    )

    # Partial unique index: one OPEN task per (task_type, related_song_id)
    op.create_index(
        "uq_tasks_open_song",
        "tasks",
        ["task_type", "related_song_id"],
        unique=True,
        postgresql_where="status = 'OPEN'::task_status AND related_song_id IS NOT NULL",
    )


def downgrade() -> None:
    op.drop_index("uq_tasks_open_video", table_name="tasks")
    op.drop_index("uq_tasks_open_song", table_name="tasks")
