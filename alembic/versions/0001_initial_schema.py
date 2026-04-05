"""Initial schema — all LOTAD tables.

Revision ID: 0001
Revises: (none)
Create Date: 2026-04-02
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

# ---------------------------------------------------------------------------
# Enum type definitions (reused across multiple tables)
# ---------------------------------------------------------------------------
# Each enum is created once in upgrade(), dropped once in downgrade().
# Using create_type=False in column definitions so Alembic doesn't try to
# create/drop them a second time when processing the table.

_ENUMS = {
    "media_type": ("GAME", "MUSIC_CD", "BOOK", "OTHER"),
    "artist_type": ("CIRCLE", "INDIVIDUAL", "UNIT"),
    "song_role": ("ARRANGER", "VOCALIST", "LYRICIST", "COMPOSER"),
    "language": (
        "JAPANESE",
        "ENGLISH",
        "CHINESE",
        "GERMAN",
        "KOREAN",
        "FRENCH",
        "INSTRUMENTAL",
        "OTHER",
    ),
    "appearance_type": (
        "PLAYABLE",
        "BOSS",
        "MIDBOSS",
        "STAGE_ENEMY",
        "SUPPORTING",
        "MENTIONED",
    ),
    "confidence_level": ("HIGH", "MEDIUM", "LOW"),
    "source_type": ("INDIVIDUAL_VIDEO", "COMPOSITE_VIDEO"),
    "task_status": ("OPEN", "IN_PROGRESS", "RESOLVED", "DISMISSED"),
    "task_type": (
        "FILL_MISSING_INFO",
        "DEDUPLICATE_SONGS",
        "REVIEW_ALBUM_TRACKS",
        "ASSIGN_PLAYLIST",
        "REVIEW_CHARACTER_MAPPING",
        "MISSING_LYRICIST",
        "MISSING_CIRCLE",
        "SUSPICIOUS_METADATA",
        "DROPPED_VIDEO",
        "REVIEW_LOCAL_TRACK",
        "INGEST_FAILED",
        "TOUHOUDB_UNREACHABLE",
    ),
    "normalization_entity_type": ("ORIGINAL_SONG", "ARTIST", "CIRCLE"),
    "physical_album_status": ("OWNED", "SOLD", "WISHLIST"),
}


def _enum(name: str, **kwargs) -> sa.Enum:
    """Return an sa.Enum that SA will auto-create when its table is created."""
    return sa.Enum(*_ENUMS[name], name=name, **kwargs)


def upgrade() -> None:
    # ------------------------------------------------------------------
    # Standalone / root tables (no FK dependencies)
    # ------------------------------------------------------------------

    op.create_table(
        "works",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("short_name", sa.Text, nullable=True),
        sa.Column("media_type", _enum("media_type"), nullable=False),
        sa.Column("release_year", sa.Integer, nullable=True),
        sa.Column("canonical_order", sa.Numeric(5, 1), nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
    )

    op.create_table(
        "artists",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("touhoudb_id", sa.Integer, nullable=True, unique=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("name_romanized", sa.Text, nullable=True),
        sa.Column("artist_type", _enum("artist_type"), nullable=False),
        sa.Column("touhoudb_url", sa.Text, nullable=True),
    )

    op.create_table(
        "albums",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("touhoudb_id", sa.Integer, nullable=True, unique=True),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("title_romanized", sa.Text, nullable=True),
        sa.Column("release_date", sa.Date, nullable=True),
        sa.Column("catalog_number", sa.Text, nullable=True),
        sa.Column("touhoudb_url", sa.Text, nullable=True),
    )

    op.create_table(
        "songs",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("touhoudb_id", sa.Integer, nullable=True, unique=True),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("title_romanized", sa.Text, nullable=True),
        sa.Column("duration_seconds", sa.Integer, nullable=True),
        sa.Column("touhoudb_url", sa.Text, nullable=True),
        sa.Column("arrangement_chronicle_url", sa.Text, nullable=True),
        sa.Column("has_lyrics", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column(
            "is_original_composition",
            sa.Boolean,
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("notes", sa.Text, nullable=True),
    )

    op.create_table(
        "youtube_videos",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("video_id", sa.Text, nullable=False, unique=True),
        sa.Column("title", sa.Text, nullable=True),
        sa.Column("channel_id", sa.Text, nullable=True),
        sa.Column("channel_name", sa.Text, nullable=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("duration_seconds", sa.Integer, nullable=True),
        sa.Column("is_available", sa.Boolean, nullable=False, server_default=sa.true()),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )

    op.create_table(
        "playlists",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("youtube_playlist_id", sa.Text, nullable=False, unique=True),
        sa.Column("display_order", sa.Integer, nullable=False),
    )

    op.create_table(
        "scoring_configurations",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.Text, nullable=False, unique=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("weights", sa.JSON, nullable=False),
        sa.Column("is_default", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )

    op.create_table(
        "normalization_metrics",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("entity_type", _enum("normalization_entity_type"), nullable=False),
        sa.Column("entity_id", sa.Integer, nullable=False),
        sa.Column("total_arrange_count", sa.Integer, nullable=False),
        sa.Column(
            "last_fetched_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("entity_type", "entity_id", name="uq_normalization_entity"),
    )

    # ------------------------------------------------------------------
    # Tables with FK dependencies on root tables
    # ------------------------------------------------------------------

    op.create_table(
        "original_songs",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("touhoudb_id", sa.Integer, nullable=True, unique=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("name_romanized", sa.Text, nullable=True),
        sa.Column(
            "work_id",
            sa.Integer,
            sa.ForeignKey("works.id", name="fk_original_songs_work_id_works"),
            nullable=False,
        ),
        sa.Column("stage", sa.Integer, nullable=True),
        sa.Column("track_number", sa.Integer, nullable=True),
        sa.Column("is_boss", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column("is_extra_stage", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column("notes", sa.Text, nullable=True),
    )

    op.create_table(
        "characters",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("name_romanized", sa.Text, nullable=True),
        sa.Column(
            "first_appearance_work_id",
            sa.Integer,
            sa.ForeignKey(
                "works.id",
                name="fk_characters_first_appearance_work_id_works",
            ),
            nullable=True,
        ),
        sa.Column("is_playable", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column("notes", sa.Text, nullable=True),
    )

    op.create_table(
        "tasks",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("task_type", _enum("task_type"), nullable=False),
        sa.Column(
            "status",
            _enum("task_status"),
            nullable=False,
            server_default="OPEN",
        ),
        sa.Column("priority", sa.Integer, nullable=False, server_default="5"),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("data", sa.JSON, nullable=False, server_default="{}"),
        sa.Column(
            "related_song_id",
            sa.Integer,
            sa.ForeignKey("songs.id", name="fk_tasks_related_song_id_songs"),
            nullable=True,
        ),
        sa.Column(
            "related_video_id",
            sa.Integer,
            sa.ForeignKey(
                "youtube_videos.id",
                name="fk_tasks_related_video_id_youtube_videos",
            ),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("auto_created_by", sa.Text, nullable=True),
    )

    op.create_table(
        "physical_albums",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "album_id",
            sa.Integer,
            sa.ForeignKey("albums.id", name="fk_physical_albums_album_id_albums"),
            nullable=True,
        ),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("circle", sa.Text, nullable=True),
        sa.Column("catalog_number", sa.Text, nullable=True),
        sa.Column(
            "status",
            _enum("physical_album_status"),
            nullable=False,
            server_default="OWNED",
        ),
    )

    # ------------------------------------------------------------------
    # Join tables
    # ------------------------------------------------------------------

    op.create_table(
        "original_song_characters",
        sa.Column(
            "original_song_id",
            sa.Integer,
            sa.ForeignKey(
                "original_songs.id",
                name="fk_original_song_characters_original_song_id_original_songs",
            ),
            nullable=False,
        ),
        sa.Column(
            "character_id",
            sa.Integer,
            sa.ForeignKey(
                "characters.id",
                name="fk_original_song_characters_character_id_characters",
            ),
            nullable=False,
        ),
        sa.Column(
            "confidence",
            _enum("confidence_level"),
            nullable=False,
            server_default="HIGH",
        ),
        sa.PrimaryKeyConstraint(
            "original_song_id", "character_id", name="pk_original_song_characters"
        ),
    )

    op.create_table(
        "character_works",
        sa.Column(
            "character_id",
            sa.Integer,
            sa.ForeignKey("characters.id", name="fk_character_works_character_id_characters"),
            nullable=False,
        ),
        sa.Column(
            "work_id",
            sa.Integer,
            sa.ForeignKey("works.id", name="fk_character_works_work_id_works"),
            nullable=False,
        ),
        sa.Column("appearance_type", _enum("appearance_type"), nullable=False),
        sa.PrimaryKeyConstraint(
            "character_id",
            "work_id",
            "appearance_type",
            name="pk_character_works",
        ),
    )

    op.create_table(
        "artist_circles",
        sa.Column(
            "artist_id",
            sa.Integer,
            sa.ForeignKey("artists.id", name="fk_artist_circles_artist_id_artists"),
            nullable=False,
        ),
        sa.Column(
            "circle_id",
            sa.Integer,
            sa.ForeignKey("artists.id", name="fk_artist_circles_circle_id_artists"),
            nullable=False,
        ),
        sa.Column("is_primary", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.PrimaryKeyConstraint("artist_id", "circle_id", name="pk_artist_circles"),
    )

    op.create_table(
        "album_circles",
        sa.Column(
            "album_id",
            sa.Integer,
            sa.ForeignKey("albums.id", name="fk_album_circles_album_id_albums"),
            nullable=False,
        ),
        sa.Column(
            "circle_id",
            sa.Integer,
            sa.ForeignKey("artists.id", name="fk_album_circles_circle_id_artists"),
            nullable=False,
        ),
        sa.Column("is_primary", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.PrimaryKeyConstraint("album_id", "circle_id", name="pk_album_circles"),
    )

    op.create_table(
        "album_events",
        sa.Column(
            "album_id",
            sa.Integer,
            sa.ForeignKey("albums.id", name="fk_album_events_album_id_albums"),
            nullable=False,
        ),
        sa.Column("event_name", sa.Text, nullable=False),
        sa.PrimaryKeyConstraint("album_id", "event_name", name="pk_album_events"),
    )

    op.create_table(
        "album_tracks",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "album_id",
            sa.Integer,
            sa.ForeignKey("albums.id", name="fk_album_tracks_album_id_albums"),
            nullable=False,
        ),
        sa.Column(
            "song_id",
            sa.Integer,
            sa.ForeignKey("songs.id", name="fk_album_tracks_song_id_songs"),
            nullable=False,
        ),
        sa.Column("track_number", sa.Integer, nullable=False),
        sa.Column("disc_number", sa.Integer, nullable=False, server_default="1"),
        sa.Column(
            "youtube_video_id",
            sa.Integer,
            sa.ForeignKey(
                "youtube_videos.id",
                name="fk_album_tracks_youtube_video_id_youtube_videos",
            ),
            nullable=True,
        ),
        sa.Column("youtube_timestamp_seconds", sa.Integer, nullable=True),
        sa.UniqueConstraint(
            "album_id",
            "disc_number",
            "track_number",
            name="uq_album_tracks_album_id",
        ),
    )

    op.create_table(
        "song_artists",
        sa.Column(
            "song_id",
            sa.Integer,
            sa.ForeignKey("songs.id", name="fk_song_artists_song_id_songs"),
            nullable=False,
        ),
        sa.Column(
            "artist_id",
            sa.Integer,
            sa.ForeignKey("artists.id", name="fk_song_artists_artist_id_artists"),
            nullable=False,
        ),
        sa.Column("role", _enum("song_role"), nullable=False),
        sa.PrimaryKeyConstraint("song_id", "artist_id", "role", name="pk_song_artists"),
    )

    op.create_table(
        "song_languages",
        sa.Column(
            "song_id",
            sa.Integer,
            sa.ForeignKey("songs.id", name="fk_song_languages_song_id_songs"),
            nullable=False,
        ),
        sa.Column("language", _enum("language"), nullable=False),
        sa.PrimaryKeyConstraint("song_id", "language", name="pk_song_languages"),
    )

    op.create_table(
        "song_originals",
        sa.Column(
            "song_id",
            sa.Integer,
            sa.ForeignKey("songs.id", name="fk_song_originals_song_id_songs"),
            nullable=False,
        ),
        sa.Column(
            "original_song_id",
            sa.Integer,
            sa.ForeignKey(
                "original_songs.id",
                name="fk_song_originals_original_song_id_original_songs",
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("song_id", "original_song_id", name="pk_song_originals"),
    )

    op.create_table(
        "song_characters",
        sa.Column(
            "song_id",
            sa.Integer,
            sa.ForeignKey("songs.id", name="fk_song_characters_song_id_songs"),
            nullable=False,
        ),
        sa.Column(
            "character_id",
            sa.Integer,
            sa.ForeignKey(
                "characters.id",
                name="fk_song_characters_character_id_characters",
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("song_id", "character_id", name="pk_song_characters"),
    )

    op.create_table(
        "physical_tracks",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "physical_album_id",
            sa.Integer,
            sa.ForeignKey(
                "physical_albums.id",
                name="fk_physical_tracks_physical_album_id_physical_albums",
            ),
            nullable=False,
        ),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("track_number", sa.Integer, nullable=False),
        sa.Column(
            "song_id",
            sa.Integer,
            sa.ForeignKey("songs.id", name="fk_physical_tracks_song_id_songs"),
            nullable=True,
        ),
    )

    op.create_table(
        "playlist_songs",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "song_id",
            sa.Integer,
            sa.ForeignKey("songs.id", name="fk_playlist_songs_song_id_songs"),
            nullable=False,
        ),
        sa.Column(
            "playlist_id",
            sa.Integer,
            sa.ForeignKey("playlists.id", name="fk_playlist_songs_playlist_id_playlists"),
            nullable=False,
        ),
        sa.Column(
            "youtube_video_id",
            sa.Integer,
            sa.ForeignKey(
                "youtube_videos.id",
                name="fk_playlist_songs_youtube_video_id_youtube_videos",
            ),
            nullable=True,
        ),
        sa.Column("youtube_timestamp_seconds", sa.Integer, nullable=True),
        sa.Column("source_type", _enum("source_type"), nullable=False),
        sa.Column("rank", sa.Integer, nullable=True),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("removed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "song_id",
            "playlist_id",
            "removed_at",
            name="uq_playlist_songs_active",
        ),
    )

    # ------------------------------------------------------------------
    # Indexes for common query patterns
    # ------------------------------------------------------------------

    op.create_index("ix_playlist_songs_playlist_id", "playlist_songs", ["playlist_id"])
    op.create_index("ix_playlist_songs_song_id", "playlist_songs", ["song_id"])
    op.create_index("ix_playlist_songs_removed_at", "playlist_songs", ["removed_at"])
    op.create_index("ix_tasks_status", "tasks", ["status"])
    op.create_index("ix_tasks_task_type", "tasks", ["task_type"])
    op.create_index(
        "ix_song_originals_original_song_id",
        "song_originals",
        ["original_song_id"],
    )
    op.create_index(
        "ix_normalization_metrics_entity_type_entity_id",
        "normalization_metrics",
        ["entity_type", "entity_id"],
    )


def downgrade() -> None:
    # Drop tables in reverse dependency order
    op.drop_table("playlist_songs")
    op.drop_table("physical_tracks")
    op.drop_table("song_characters")
    op.drop_table("song_originals")
    op.drop_table("song_languages")
    op.drop_table("song_artists")
    op.drop_table("album_tracks")
    op.drop_table("album_events")
    op.drop_table("album_circles")
    op.drop_table("artist_circles")
    op.drop_table("character_works")
    op.drop_table("original_song_characters")
    op.drop_table("physical_albums")
    op.drop_table("tasks")
    op.drop_table("characters")
    op.drop_table("original_songs")
    op.drop_table("normalization_metrics")
    op.drop_table("scoring_configurations")
    op.drop_table("playlists")
    op.drop_table("youtube_videos")
    op.drop_table("songs")
    op.drop_table("albums")
    op.drop_table("artists")
    op.drop_table("works")

    # Drop enum types (no-op for SQLite)
    bind = op.get_bind()
    for name, values in _ENUMS.items():
        sa.Enum(*values, name=name).drop(bind, checkfirst=True)
