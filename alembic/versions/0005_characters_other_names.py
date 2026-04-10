"""Add other_names array column to characters

Revision ID: 0005
Revises: 0004
Create Date: 2026-04-10
"""

from alembic import op
import sqlalchemy as sa

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("characters", sa.Column("other_names", sa.ARRAY(sa.Text), nullable=True))


def downgrade() -> None:
    op.drop_column("characters", "other_names")
