"""Add MUSIC_PV value to song_type enum.

Songs with an official music video released by the circle are tagged
'MusicPV' in TouhouDB.  Previously these were collapsed into OTHER; this
gives them a dedicated value so they can be queried independently while
still being included naturally in broad arrangement queries.

NOTE: PostgreSQL does not support removing enum values, so the downgrade
is intentionally a no-op.  To fully roll back you would need to recreate
the enum type and migrate existing rows — not worth the complexity for an
additive change.

Revision ID: 0004
Revises: 0003
Create Date: 2026-04-08
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision: str = "0004"
down_revision: str = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ADD VALUE cannot run inside a transaction in PostgreSQL, so we use an
    # autocommit block.  IF NOT EXISTS makes the migration re-runnable safely.
    with op.get_context().autocommit_block():
        op.execute(sa.text("ALTER TYPE song_type ADD VALUE IF NOT EXISTS 'MUSIC_PV'"))


def downgrade() -> None:
    # PostgreSQL does not support removing enum values without recreating the
    # type.  Leave the value in place — it is harmless if unused.
    pass
