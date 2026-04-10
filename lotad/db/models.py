"""SQLAlchemy Core table definitions for LOTAD.

All tables are defined here and referenced by Alembic migrations.
Using Core (Table/Column) rather than ORM (declarative) to keep
complex SQL queries readable and avoid ORM magic in a pipeline context.
"""

from __future__ import annotations

import enum

import sqlalchemy as sa
from sqlalchemy import MetaData

# Naming convention for constraints — Alembic uses this for autogenerate
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=convention)

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class MediaType(enum.StrEnum):
    GAME = "GAME"
    MUSIC_CD = "MUSIC_CD"
    BOOK = "BOOK"
    OTHER = "OTHER"


class ArtistType(enum.StrEnum):
    CIRCLE = "CIRCLE"
    LABEL = "LABEL"  # distributes/releases music but doesn't produce it
    INDIVIDUAL = "INDIVIDUAL"  # producer / arranger not primarily associated with a circle
    VOCALIST = "VOCALIST"  # human vocalist (solo, not bound to one circle)
    UNIT = "UNIT"  # ad-hoc collaboration group


class SongRole(enum.StrEnum):
    ARRANGER = "ARRANGER"
    COMPOSER = "COMPOSER"
    VOCALIST = "VOCALIST"
    LYRICIST = "LYRICIST"
    INSTRUMENTALIST = "INSTRUMENTALIST"
    MIXER = "MIXER"
    MASTERING = "MASTERING"
    CHORUS = "CHORUS"  # backing vocals


class SongType(enum.StrEnum):
    """Mirrors TouhouDB SongType. Most LOTAD entries will be ARRANGEMENT."""

    ARRANGEMENT = "ARRANGEMENT"  # standard Touhou arrangement
    REARRANGEMENT = "REARRANGEMENT"  # arrange of an existing arrangement (TouhouDB-specific)
    REMIX = "REMIX"
    COVER = "COVER"
    MASHUP = "MASHUP"
    INSTRUMENTAL = "INSTRUMENTAL"  # vocal version exists but this is the instrumental
    ORIGINAL = "ORIGINAL"  # original composition (not a Touhou arrangement)
    REMASTER = "REMASTER"
    LIVE = "LIVE"
    SHORT_VERSION = "SHORT_VERSION"  # TouhouDB-specific
    MUSIC_PV = "MUSIC_PV"  # arrangement with an official music video
    OTHER = "OTHER"


class DiscType(enum.StrEnum):
    """Album release format. Mirrors TouhouDB DiscType."""

    ALBUM = "ALBUM"
    SINGLE = "SINGLE"
    EP = "EP"
    SPLIT = "SPLIT"
    COMPILATION = "COMPILATION"
    GAME = "GAME"  # TouhouDB-specific — ZUN's game OSTs
    FANMADE = "FANMADE"  # TouhouDB-specific — unofficial fan game OSTs
    INSTRUMENTAL = "INSTRUMENTAL"  # all-instrumental album
    VIDEO = "VIDEO"
    OTHER = "OTHER"


class Language(enum.StrEnum):
    JAPANESE = "JAPANESE"
    ENGLISH = "ENGLISH"
    CHINESE = "CHINESE"
    GERMAN = "GERMAN"
    KOREAN = "KOREAN"
    FRENCH = "FRENCH"
    INSTRUMENTAL = "INSTRUMENTAL"
    OTHER = "OTHER"


class AppearanceType(enum.StrEnum):
    PLAYABLE = "PLAYABLE"
    BOSS = "BOSS"
    MIDBOSS = "MIDBOSS"
    STAGE_ENEMY = "STAGE_ENEMY"
    SUPPORTING = "SUPPORTING"
    MENTIONED = "MENTIONED"


class ConfidenceLevel(enum.StrEnum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class SourceType(enum.StrEnum):
    INDIVIDUAL_VIDEO = "INDIVIDUAL_VIDEO"
    COMPOSITE_VIDEO = "COMPOSITE_VIDEO"


class TaskStatus(enum.StrEnum):
    OPEN = "OPEN"
    IN_PROGRESS = "IN_PROGRESS"
    RESOLVED = "RESOLVED"
    DISMISSED = "DISMISSED"


class TaskType(enum.StrEnum):
    FILL_MISSING_INFO = "FILL_MISSING_INFO"
    DEDUPLICATE_SONGS = "DEDUPLICATE_SONGS"
    REVIEW_ALBUM_TRACKS = "REVIEW_ALBUM_TRACKS"
    ASSIGN_PLAYLIST = "ASSIGN_PLAYLIST"
    REVIEW_CHARACTER_MAPPING = "REVIEW_CHARACTER_MAPPING"
    MISSING_LYRICIST = "MISSING_LYRICIST"
    MISSING_CIRCLE = "MISSING_CIRCLE"
    SUSPICIOUS_METADATA = "SUSPICIOUS_METADATA"
    DROPPED_VIDEO = "DROPPED_VIDEO"
    REVIEW_LOCAL_TRACK = "REVIEW_LOCAL_TRACK"
    INGEST_FAILED = "INGEST_FAILED"
    TOUHOUDB_UNREACHABLE = "TOUHOUDB_UNREACHABLE"


class NormalizationEntityType(enum.StrEnum):
    ORIGINAL_SONG = "ORIGINAL_SONG"
    ARTIST = "ARTIST"
    CIRCLE = "CIRCLE"


class PhysicalAlbumStatus(enum.StrEnum):
    OWNED = "OWNED"
    SOLD = "SOLD"
    WISHLIST = "WISHLIST"


# ---------------------------------------------------------------------------
# SQLAlchemy enum types (reusable across column defs)
# ---------------------------------------------------------------------------

media_type_enum = sa.Enum(
    MediaType, name="media_type", values_callable=lambda x: [e.value for e in x]
)
artist_type_enum = sa.Enum(
    ArtistType, name="artist_type", values_callable=lambda x: [e.value for e in x]
)
song_role_enum = sa.Enum(SongRole, name="song_role", values_callable=lambda x: [e.value for e in x])
language_enum = sa.Enum(Language, name="language", values_callable=lambda x: [e.value for e in x])
appearance_type_enum = sa.Enum(
    AppearanceType,
    name="appearance_type",
    values_callable=lambda x: [e.value for e in x],
)
confidence_enum = sa.Enum(
    ConfidenceLevel,
    name="confidence_level",
    values_callable=lambda x: [e.value for e in x],
)
source_type_enum = sa.Enum(
    SourceType, name="source_type", values_callable=lambda x: [e.value for e in x]
)
task_status_enum = sa.Enum(
    TaskStatus, name="task_status", values_callable=lambda x: [e.value for e in x]
)
task_type_enum = sa.Enum(TaskType, name="task_type", values_callable=lambda x: [e.value for e in x])
normalization_entity_type_enum = sa.Enum(
    NormalizationEntityType,
    name="normalization_entity_type",
    values_callable=lambda x: [e.value for e in x],
)
physical_album_status_enum = sa.Enum(
    PhysicalAlbumStatus,
    name="physical_album_status",
    values_callable=lambda x: [e.value for e in x],
)
song_type_enum = sa.Enum(SongType, name="song_type", values_callable=lambda x: [e.value for e in x])
disc_type_enum = sa.Enum(DiscType, name="disc_type", values_callable=lambda x: [e.value for e in x])

# ---------------------------------------------------------------------------
# Core lookup / reference tables
# ---------------------------------------------------------------------------

works = sa.Table(
    "works",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("short_name", sa.Text, nullable=True),  # e.g. "PCB", "IN"
    sa.Column("media_type", media_type_enum, nullable=False),
    sa.Column("release_year", sa.Integer, nullable=True),
    # Ordering within canonical games sequence (null for books / music CDs)
    sa.Column("canonical_order", sa.Numeric(5, 1), nullable=True),
    sa.Column("notes", sa.Text, nullable=True),
)

original_songs = sa.Table(
    "original_songs",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("touhoudb_id", sa.Integer, nullable=True, unique=True),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("name_romanized", sa.Text, nullable=True),
    sa.Column("work_id", sa.Integer, sa.ForeignKey("works.id"), nullable=False),
    sa.Column("stage", sa.Integer, nullable=True),
    sa.Column("track_number", sa.Integer, nullable=True),
    sa.Column("is_boss", sa.Boolean, nullable=False, server_default="false"),
    sa.Column("is_extra_stage", sa.Boolean, nullable=False, server_default="false"),
    sa.Column("notes", sa.Text, nullable=True),
)

characters = sa.Table(
    "characters",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("name_romanized", sa.Text, nullable=True),
    sa.Column("other_names", sa.ARRAY(sa.Text), nullable=True),
    sa.Column(
        "first_appearance_work_id",
        sa.Integer,
        sa.ForeignKey("works.id"),
        nullable=True,
    ),
    sa.Column("is_playable", sa.Boolean, nullable=False, server_default="false"),
    sa.Column("notes", sa.Text, nullable=True),
    # TouhouDB artist ID for characters; used to upsert from API responses.
    # Nullable because manually-seeded characters won't have a TouhouDB entry.
    sa.Column("touhoudb_id", sa.Integer, nullable=True, unique=True),
)

artists = sa.Table(
    "artists",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("touhoudb_id", sa.Integer, nullable=True, unique=True),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("name_romanized", sa.Text, nullable=True),
    sa.Column("artist_type", artist_type_enum, nullable=False),
    sa.Column("touhoudb_url", sa.Text, nullable=True),
)

albums = sa.Table(
    "albums",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("touhoudb_id", sa.Integer, nullable=True, unique=True),
    sa.Column("title", sa.Text, nullable=False),
    sa.Column("title_romanized", sa.Text, nullable=True),
    sa.Column("release_date", sa.Date, nullable=True),
    sa.Column("catalog_number", sa.Text, nullable=True),
    sa.Column("touhoudb_url", sa.Text, nullable=True),
    # TouhouDB-aligned fields
    sa.Column("disc_type", disc_type_enum, nullable=False, server_default="ALBUM"),
    sa.Column("description", sa.Text, nullable=True),
    sa.Column("barcode", sa.Text, nullable=True),
)

songs = sa.Table(
    "songs",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("touhoudb_id", sa.Integer, nullable=True, unique=True),
    sa.Column("title", sa.Text, nullable=False),
    sa.Column("title_romanized", sa.Text, nullable=True),
    sa.Column("duration_seconds", sa.Integer, nullable=True),
    sa.Column("touhoudb_url", sa.Text, nullable=True),
    sa.Column("arrangement_chronicle_url", sa.Text, nullable=True),
    sa.Column("has_lyrics", sa.Boolean, nullable=False, server_default="false"),
    sa.Column("is_original_composition", sa.Boolean, nullable=False, server_default="false"),
    # TouhouDB-aligned fields
    sa.Column(
        "song_type",
        song_type_enum,
        nullable=False,
        server_default="ARRANGEMENT",
    ),
    sa.Column("publish_date", sa.Date, nullable=True),
    # BPM stored as millibpm integers (e.g. 174000 = 174 BPM) — mirrors TouhouDB
    sa.Column("min_milli_bpm", sa.Integer, nullable=True),
    sa.Column("max_milli_bpm", sa.Integer, nullable=True),
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
        onupdate=sa.func.now(),
    ),
    sa.Column("notes", sa.Text, nullable=True),
)

youtube_videos = sa.Table(
    "youtube_videos",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("video_id", sa.Text, nullable=False, unique=True),  # YT 11-char ID
    sa.Column("title", sa.Text, nullable=True),
    sa.Column("channel_id", sa.Text, nullable=True),
    sa.Column("channel_name", sa.Text, nullable=True),
    sa.Column("description", sa.Text, nullable=True),
    sa.Column("duration_seconds", sa.Integer, nullable=True),
    sa.Column("is_available", sa.Boolean, nullable=False, server_default="true"),
    # last_checked_at: updated every sync run (availability poll)
    sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
    # updated_at: only changes when a data field actually changes
    sa.Column(
        "updated_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
)

playlists = sa.Table(
    "playlists",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.Text, nullable=False),
    sa.Column("youtube_playlist_id", sa.Text, nullable=False, unique=True),
    sa.Column("display_order", sa.Integer, nullable=False),
)

scoring_configurations = sa.Table(
    "scoring_configurations",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.Text, nullable=False, unique=True),
    sa.Column("description", sa.Text, nullable=True),
    # JSONB: {"TOUHOU MEGAMIX": 10, "pq": 8.5, ...}
    sa.Column("weights", sa.JSON, nullable=False),
    sa.Column("is_default", sa.Boolean, nullable=False, server_default="false"),
    sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
)

tasks = sa.Table(
    "tasks",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("task_type", task_type_enum, nullable=False),
    sa.Column(
        "status",
        task_status_enum,
        nullable=False,
        server_default="OPEN",
    ),
    sa.Column("priority", sa.Integer, nullable=False, server_default="5"),
    sa.Column("title", sa.Text, nullable=False),
    sa.Column("data", sa.JSON, nullable=False, server_default="{}"),
    sa.Column("related_song_id", sa.Integer, sa.ForeignKey("songs.id"), nullable=True),
    sa.Column(
        "related_video_id",
        sa.Integer,
        sa.ForeignKey("youtube_videos.id"),
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

normalization_metrics = sa.Table(
    "normalization_metrics",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("entity_type", normalization_entity_type_enum, nullable=False),
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

physical_albums = sa.Table(
    "physical_albums",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    # Nullable: physical album may not be matched to a TouhouDB album
    sa.Column("album_id", sa.Integer, sa.ForeignKey("albums.id"), nullable=True),
    sa.Column("title", sa.Text, nullable=False),
    sa.Column("circle", sa.Text, nullable=True),
    sa.Column("catalog_number", sa.Text, nullable=True),
    sa.Column("status", physical_album_status_enum, nullable=False, server_default="OWNED"),
)

physical_tracks = sa.Table(
    "physical_tracks",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column(
        "physical_album_id",
        sa.Integer,
        sa.ForeignKey("physical_albums.id"),
        nullable=False,
    ),
    sa.Column("title", sa.Text, nullable=False),
    sa.Column("track_number", sa.Integer, nullable=False),
    # Nullable: physical track may not be matched to a song in the DB
    sa.Column("song_id", sa.Integer, sa.ForeignKey("songs.id"), nullable=True),
)

# ---------------------------------------------------------------------------
# Join tables
# ---------------------------------------------------------------------------

original_song_characters = sa.Table(
    "original_song_characters",
    metadata,
    sa.Column(
        "original_song_id",
        sa.Integer,
        sa.ForeignKey("original_songs.id"),
        nullable=False,
    ),
    sa.Column("character_id", sa.Integer, sa.ForeignKey("characters.id"), nullable=False),
    sa.Column("confidence", confidence_enum, nullable=False, server_default="HIGH"),
    sa.PrimaryKeyConstraint("original_song_id", "character_id"),
)

character_works = sa.Table(
    "character_works",
    metadata,
    sa.Column("character_id", sa.Integer, sa.ForeignKey("characters.id"), nullable=False),
    sa.Column("work_id", sa.Integer, sa.ForeignKey("works.id"), nullable=False),
    sa.Column("appearance_type", appearance_type_enum, nullable=False),
    sa.PrimaryKeyConstraint("character_id", "work_id", "appearance_type"),
)

artist_circles = sa.Table(
    "artist_circles",
    metadata,
    sa.Column("artist_id", sa.Integer, sa.ForeignKey("artists.id"), nullable=False),
    # circle_id references artists (circles are also rows in the artists table)
    sa.Column("circle_id", sa.Integer, sa.ForeignKey("artists.id"), nullable=False),
    sa.Column("is_primary", sa.Boolean, nullable=False, server_default="false"),
    sa.PrimaryKeyConstraint("artist_id", "circle_id"),
)

album_circles = sa.Table(
    "album_circles",
    metadata,
    sa.Column("album_id", sa.Integer, sa.ForeignKey("albums.id"), nullable=False),
    sa.Column("circle_id", sa.Integer, sa.ForeignKey("artists.id"), nullable=False),
    sa.Column("is_primary", sa.Boolean, nullable=False, server_default="false"),
    sa.PrimaryKeyConstraint("album_id", "circle_id"),
)

album_events = sa.Table(
    "album_events",
    metadata,
    sa.Column("album_id", sa.Integer, sa.ForeignKey("albums.id"), nullable=False),
    sa.Column("event_name", sa.Text, nullable=False),
    sa.PrimaryKeyConstraint("album_id", "event_name"),
)

album_tracks = sa.Table(
    "album_tracks",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("album_id", sa.Integer, sa.ForeignKey("albums.id"), nullable=False),
    sa.Column("song_id", sa.Integer, sa.ForeignKey("songs.id"), nullable=False),
    sa.Column("track_number", sa.Integer, nullable=False),
    sa.Column("disc_number", sa.Integer, nullable=False, server_default="1"),
    sa.Column(
        "youtube_video_id",
        sa.Integer,
        sa.ForeignKey("youtube_videos.id"),
        nullable=True,
    ),
    sa.Column("youtube_timestamp_seconds", sa.Integer, nullable=True),
    sa.UniqueConstraint("album_id", "disc_number", "track_number"),
)

song_artists = sa.Table(
    "song_artists",
    metadata,
    sa.Column("song_id", sa.Integer, sa.ForeignKey("songs.id"), nullable=False),
    sa.Column("artist_id", sa.Integer, sa.ForeignKey("artists.id"), nullable=False),
    sa.Column("role", song_role_enum, nullable=False),
    sa.PrimaryKeyConstraint("song_id", "artist_id", "role"),
)

song_languages = sa.Table(
    "song_languages",
    metadata,
    sa.Column("song_id", sa.Integer, sa.ForeignKey("songs.id"), nullable=False),
    sa.Column("language", language_enum, nullable=False),
    sa.PrimaryKeyConstraint("song_id", "language"),
)

song_originals = sa.Table(
    "song_originals",
    metadata,
    sa.Column("song_id", sa.Integer, sa.ForeignKey("songs.id"), nullable=False),
    sa.Column(
        "original_song_id",
        sa.Integer,
        sa.ForeignKey("original_songs.id"),
        nullable=False,
    ),
    sa.PrimaryKeyConstraint("song_id", "original_song_id"),
)

song_characters = sa.Table(
    "song_characters",
    metadata,
    sa.Column("song_id", sa.Integer, sa.ForeignKey("songs.id"), nullable=False),
    sa.Column("character_id", sa.Integer, sa.ForeignKey("characters.id"), nullable=False),
    sa.PrimaryKeyConstraint("song_id", "character_id"),
)

# Tags: freeform genre/mood labels (mirrors TouhouDB's tag system)
# Using text rather than a foreign key to a tags table — LOTAD doesn't need
# a curated tag vocabulary; tags flow in from TouhouDB as-is.
song_tags = sa.Table(
    "song_tags",
    metadata,
    sa.Column("song_id", sa.Integer, sa.ForeignKey("songs.id"), nullable=False),
    sa.Column("tag", sa.Text, nullable=False),
    sa.Column("count", sa.Integer, nullable=False, server_default="1"),
    sa.PrimaryKeyConstraint("song_id", "tag"),
)

album_tags = sa.Table(
    "album_tags",
    metadata,
    sa.Column("album_id", sa.Integer, sa.ForeignKey("albums.id"), nullable=False),
    sa.Column("tag", sa.Text, nullable=False),
    sa.Column("count", sa.Integer, nullable=False, server_default="1"),
    sa.PrimaryKeyConstraint("album_id", "tag"),
)

playlist_songs = sa.Table(
    "playlist_songs",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("song_id", sa.Integer, sa.ForeignKey("songs.id"), nullable=False),
    sa.Column("playlist_id", sa.Integer, sa.ForeignKey("playlists.id"), nullable=False),
    sa.Column(
        "youtube_video_id",
        sa.Integer,
        sa.ForeignKey("youtube_videos.id"),
        nullable=True,
    ),
    sa.Column("youtube_timestamp_seconds", sa.Integer, nullable=True),
    sa.Column("source_type", source_type_enum, nullable=False),
    # Fine-grained user rank within the playlist (set via sorter / CLI)
    sa.Column("rank", sa.Integer, nullable=True),
    sa.Column(
        "added_at",
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    ),
    # Non-null when a video is removed from the playlist
    sa.Column("removed_at", sa.DateTime(timezone=True), nullable=True),
    # Prevent duplicate active entries for the same song+playlist combination
    sa.UniqueConstraint(
        "song_id",
        "playlist_id",
        "removed_at",
        name="uq_playlist_songs_active",
    ),
)
