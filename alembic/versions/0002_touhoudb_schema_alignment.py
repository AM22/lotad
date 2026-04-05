"""TouhouDB-aligned schema additions.

Adds:
- `song_type` enum + column on `songs`
- `publish_date`, `min_milli_bpm`, `max_milli_bpm` columns on `songs`
- `disc_type` enum + column on `albums`
- `description`, `barcode` columns on `albums`
- `song_tags` table
- `album_tags` table
- Expanded `artist_type` enum (LABEL, VOCALIST added)
- Expanded `song_role` enum (INSTRUMENTALIST, MIXER, MASTERING, CHORUS added)

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-05
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic
revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

# ---------------------------------------------------------------------------
# New enum definitions
# ---------------------------------------------------------------------------

song_type = sa.Enum(
    "ARRANGEMENT",
    "REARRANGEMENT",
    "REMIX",
    "COVER",
    "MASHUP",
    "INSTRUMENTAL",
    "ORIGINAL",
    "REMASTER",
    "LIVE",
    "SHORT_VERSION",
    "OTHER",
    name="song_type",
)

disc_type = sa.Enum(
    "ALBUM",
    "SINGLE",
    "EP",
    "SPLIT",
    "COMPILATION",
    "GAME",
    "FANMADE",
    "INSTRUMENTAL",
    "VIDEO",
    "OTHER",
    name="disc_type",
)


def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. Create new enum types in Postgres
    # ------------------------------------------------------------------
    song_type.create(op.get_bind(), checkfirst=True)
    disc_type.create(op.get_bind(), checkfirst=True)

    # ------------------------------------------------------------------
    # 2. Expand artist_type enum (add LABEL, VOCALIST)
    #    PostgreSQL requires ALTER TYPE ... ADD VALUE (cannot be done in a
    #    transaction for older PG; use checkfirst=True to be safe).
    # ------------------------------------------------------------------
    op.execute("ALTER TYPE artist_type ADD VALUE IF NOT EXISTS 'LABEL'")
    op.execute("ALTER TYPE artist_type ADD VALUE IF NOT EXISTS 'VOCALIST'")

    # ------------------------------------------------------------------
    # 3. Expand song_role enum (add INSTRUMENTALIST, MIXER, MASTERING, CHORUS)
    # ------------------------------------------------------------------
    op.execute("ALTER TYPE song_role ADD VALUE IF NOT EXISTS 'INSTRUMENTALIST'")
    op.execute("ALTER TYPE song_role ADD VALUE IF NOT EXISTS 'MIXER'")
    op.execute("ALTER TYPE song_role ADD VALUE IF NOT EXISTS 'MASTERING'")
    op.execute("ALTER TYPE song_role ADD VALUE IF NOT EXISTS 'CHORUS'")

    # ------------------------------------------------------------------
    # 4. Add new columns to `songs`
    # ------------------------------------------------------------------
    op.add_column(
        "songs",
        sa.Column(
            "song_type",
            song_type,
            nullable=False,
            server_default="ARRANGEMENT",
        ),
    )
    op.add_column("songs", sa.Column("publish_date", sa.Date, nullable=True))
    op.add_column("songs", sa.Column("min_milli_bpm", sa.Integer, nullable=True))
    op.add_column("songs", sa.Column("max_milli_bpm", sa.Integer, nullable=True))

    # ------------------------------------------------------------------
    # 5. Add new columns to `albums`
    # ------------------------------------------------------------------
    op.add_column(
        "albums",
        sa.Column(
            "disc_type",
            disc_type,
            nullable=False,
            server_default="ALBUM",
        ),
    )
    op.add_column("albums", sa.Column("description", sa.Text, nullable=True))
    op.add_column("albums", sa.Column("barcode", sa.Text, nullable=True))

    # ------------------------------------------------------------------
    # 6. Create song_tags table
    # ------------------------------------------------------------------
    op.create_table(
        "song_tags",
        sa.Column("song_id", sa.Integer, sa.ForeignKey("songs.id"), nullable=False),
        sa.Column("tag", sa.Text, nullable=False),
        sa.Column("count", sa.Integer, nullable=False, server_default="1"),
        sa.PrimaryKeyConstraint("song_id", "tag", name="pk_song_tags"),
    )

    # ------------------------------------------------------------------
    # 7. Create album_tags table
    # ------------------------------------------------------------------
    op.create_table(
        "album_tags",
        sa.Column("album_id", sa.Integer, sa.ForeignKey("albums.id"), nullable=False),
        sa.Column("tag", sa.Text, nullable=False),
        sa.Column("count", sa.Integer, nullable=False, server_default="1"),
        sa.PrimaryKeyConstraint("album_id", "tag", name="pk_album_tags"),
    )


def downgrade() -> None:
    # Reverse in opposite order

    # 7. Drop album_tags
    op.drop_table("album_tags")

    # 6. Drop song_tags
    op.drop_table("song_tags")

    # 5. Drop albums columns
    op.drop_column("albums", "barcode")
    op.drop_column("albums", "description")
    op.drop_column("albums", "disc_type")

    # 4. Drop songs columns
    op.drop_column("songs", "max_milli_bpm")
    op.drop_column("songs", "min_milli_bpm")
    op.drop_column("songs", "publish_date")
    op.drop_column("songs", "song_type")

    # 3. Note: PostgreSQL does not support removing enum values — no downgrade
    #    for the expanded song_role / artist_type enums.

    # 2. Drop new enum types
    disc_type.drop(op.get_bind(), checkfirst=True)
    song_type.drop(op.get_bind(), checkfirst=True)
