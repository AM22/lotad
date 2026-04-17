"""Change tasks.data column from json to jsonb

json does not support the || merge operator; jsonb does.
This is a safe in-place cast — all existing json values are valid jsonb.

Revision ID: 0012
Revises: 0011
Create Date: 2026-04-17
"""

from alembic import op

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE tasks ALTER COLUMN data TYPE jsonb USING data::jsonb")


def downgrade() -> None:
    op.execute("ALTER TABLE tasks ALTER COLUMN data TYPE json USING data::json")
