"""Add touhoudb_id to works table

Stores the TouhouDB album ID for the canonical album representing each work.
Populated by `lotad originals scrape` when a confident match is found.
Null for works that haven't been matched yet.

Revision ID: 0007
Revises: 0006
Create Date: 2026-04-11
"""

import sqlalchemy as sa
from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("works", sa.Column("touhoudb_id", sa.Integer(), nullable=True))
    op.create_unique_constraint("uq_works_touhoudb_id", "works", ["touhoudb_id"])


def downgrade() -> None:
    op.drop_constraint("uq_works_touhoudb_id", "works", type_="unique")
    op.drop_column("works", "touhoudb_id")
