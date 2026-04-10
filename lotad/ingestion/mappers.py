"""Map TouhouDB API responses to LOTAD database upserts.

All public functions accept an open SQLAlchemy ``Connection`` (Core, not ORM)
and operate within the caller's transaction.  The caller is responsible for
commit/rollback.

Design decisions:
- Upserts use PostgreSQL ``INSERT … ON CONFLICT DO UPDATE`` keyed on
  ``touhoudb_id``.
- ``song_originals`` is only populated when the original song already has a
  matching ``touhoudb_id`` in our ``original_songs`` table.  If it doesn't, a
  ``FILL_MISSING_INFO`` task is created so a human can link it manually.
- ``song_languages`` is not populated here (TouhouDB doesn't expose language
  per-song in a reliable way; M4 LLM extraction will handle this).
- ``artist_circles`` membership is populated from ``ArtistForSong.artist``
  group info only when a full artist detail is fetched.  Basic upserts here
  skip the group traversal to keep M2 scope bounded.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Connection
from sqlalchemy.dialects.postgresql import insert as pg_insert

from lotad.db.models import (
    ArtistType,
    DiscType,
    SongRole,
    SongType,
    album_circles,
    album_tags,
    albums,
    artists,
    characters,
    original_songs,
    song_artists,
    song_characters,
    song_tags,
    songs,
)
from lotad.ingestion.touhoudb_models import AlbumDetail, ArtistForAlbum, ArtistForSong, SongDetail

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Type mapping helpers
# ---------------------------------------------------------------------------

_SONG_TYPE_MAP: dict[str, SongType] = {
    "arrangement": SongType.ARRANGEMENT,
    "rearrangement": SongType.REARRANGEMENT,
    "remix": SongType.REMIX,
    "cover": SongType.COVER,
    "mashup": SongType.MASHUP,
    "instrumental": SongType.INSTRUMENTAL,
    "original": SongType.ORIGINAL,
    "remaster": SongType.REMASTER,
    # VocaDB/TouhouDB types that don't map cleanly to a musical category
    "musicpv": SongType.MUSIC_PV,
    "dramapv": SongType.OTHER,
    "unspecified": SongType.OTHER,
    "other": SongType.OTHER,
}

_DISC_TYPE_MAP: dict[str, DiscType] = {
    "album": DiscType.ALBUM,
    "single": DiscType.SINGLE,
    "ep": DiscType.EP,
    "split": DiscType.SPLIT,
    "compilation": DiscType.COMPILATION,
    "game": DiscType.GAME,
    "fanmade": DiscType.FANMADE,
    "instrumental": DiscType.INSTRUMENTAL,
    "video": DiscType.VIDEO,
    "artbook": DiscType.OTHER,
    "other": DiscType.OTHER,
}

_ARTIST_TYPE_MAP: dict[str, ArtistType] = {
    "circle": ArtistType.CIRCLE,
    "label": ArtistType.LABEL,
    "producer": ArtistType.INDIVIDUAL,
    "animator": ArtistType.INDIVIDUAL,
    "illustrator": ArtistType.INDIVIDUAL,
    "lyricist": ArtistType.INDIVIDUAL,
    "otherindividual": ArtistType.INDIVIDUAL,
    "vocalist": ArtistType.VOCALIST,
    "band": ArtistType.UNIT,
    "othergroup": ArtistType.UNIT,
    "unknown": ArtistType.INDIVIDUAL,
    # "Character" entries are Touhou game characters used as subject tags —
    # they should be filtered before reaching _upsert_artist, but map them
    # explicitly so the fallback warning is never triggered for this type.
    "character": ArtistType.INDIVIDUAL,
}

_ROLE_MAP: dict[str, SongRole] = {
    "arranger": SongRole.ARRANGER,
    "composer": SongRole.COMPOSER,
    "vocalist": SongRole.VOCALIST,
    "lyricist": SongRole.LYRICIST,
    "instrumentalist": SongRole.INSTRUMENTALIST,
    "mixer": SongRole.MIXER,
    "mastering": SongRole.MASTERING,
    # "chorus" is not a standard VocaDB role, but include for completeness
}


def _map_song_type(raw: str) -> SongType:
    result = _SONG_TYPE_MAP.get(raw.lower())
    if result is None:
        logger.warning("Unknown TouhouDB songType %r — falling back to OTHER", raw)
        return SongType.OTHER
    return result


def _map_disc_type(raw: str) -> DiscType:
    return _DISC_TYPE_MAP.get(raw.lower(), DiscType.OTHER)


def _map_artist_type(raw: str) -> ArtistType:
    return _ARTIST_TYPE_MAP.get(raw.lower(), ArtistType.INDIVIDUAL)


def _map_role(raw: str, *, artist_type: str = "Unknown") -> SongRole | None:
    """
    Map a single TouhouDB role string to our SongRole enum.

    "Default" in VocaDB/TouhouDB means "infer the role from the artist's type"
    rather than being an explicit Arranger credit.  For Vocalist-type artists the
    inferred role is VOCALIST; for everyone else (producers, circles, individuals,
    groups) it is ARRANGER.

    Returns None for unmapped roles (the caller will log and skip them).
    """
    normalized = raw.strip().lower()
    if normalized == "default":
        return SongRole.VOCALIST if artist_type.lower() == "vocalist" else SongRole.ARRANGER
    return _ROLE_MAP.get(normalized)


def _parse_publish_date(raw: str | None) -> date | None:
    """Parse an ISO-8601 datetime string to a date object."""
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])  # take YYYY-MM-DD prefix
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Artist upsert
# ---------------------------------------------------------------------------


def _upsert_artist(credit: ArtistForSong | ArtistForAlbum, conn: Connection) -> int | None:
    """
    Upsert an artist record from a song/album credit.

    Returns the internal artist ``id``, or ``None`` if no artist info is
    available (e.g. custom-name-only credits with no linked artist object).
    """
    artist_summary = credit.artist
    if artist_summary is None:
        return None

    artist_type = _map_artist_type(artist_summary.artistType)
    stmt = (
        pg_insert(artists)
        .values(
            touhoudb_id=artist_summary.id,
            name=artist_summary.name,
            artist_type=artist_type,
            touhoudb_url=f"https://touhoudb.com/Ar/{artist_summary.id}",
        )
        .on_conflict_do_update(
            index_elements=["touhoudb_id"],
            set_={
                "name": artist_summary.name,
                "artist_type": artist_type,
            },
        )
        .returning(artists.c.id)
    )
    return conn.execute(stmt).scalar_one()


# ---------------------------------------------------------------------------
# Song mapper
# ---------------------------------------------------------------------------


def map_song_to_db(detail: SongDetail, conn: Connection) -> int:
    """
    Upsert a song and its related rows from a ``SongDetail`` response.

    Touches: ``songs``, ``artists``, ``song_artists``, ``song_tags``.
    Does NOT touch: ``song_originals`` (requires original song lookup —
    handled in pipeline.py), ``song_languages`` (deferred to M4).

    Returns the internal ``songs.id``.
    """
    song_type = _map_song_type(detail.songType)
    has_lyrics = detail.has_lyrics
    is_orig = detail.is_original_composition

    # 1. Upsert the song row
    stmt = (
        pg_insert(songs)
        .values(
            touhoudb_id=detail.id,
            title=detail.name,
            duration_seconds=detail.lengthSeconds,
            has_lyrics=has_lyrics,
            is_original_composition=is_orig,
            song_type=song_type,
            publish_date=_parse_publish_date(detail.publishDate),
            min_milli_bpm=detail.minMilliBpm,
            max_milli_bpm=detail.maxMilliBpm,
            touhoudb_url=f"https://touhoudb.com/S/{detail.id}",
        )
        .on_conflict_do_update(
            index_elements=["touhoudb_id"],
            set_={
                "title": detail.name,
                "duration_seconds": detail.lengthSeconds,
                "has_lyrics": has_lyrics,
                "is_original_composition": is_orig,
                "song_type": song_type,
                "publish_date": _parse_publish_date(detail.publishDate),
                "min_milli_bpm": detail.minMilliBpm,
                "max_milli_bpm": detail.maxMilliBpm,
                "updated_at": sa.func.now(),
            },
        )
        .returning(songs.c.id)
    )
    song_id: int = conn.execute(stmt).scalar_one()

    # 2. Replace artist credits and character links.
    # Delete-then-reinsert (within the same transaction) ensures stale credits
    # from a previous ingest are removed if TouhouDB data has changed.
    conn.execute(song_artists.delete().where(song_artists.c.song_id == song_id))
    conn.execute(song_characters.delete().where(song_characters.c.song_id == song_id))
    _upsert_song_artists(detail.artists, song_id, conn)

    # 3. Replace tags (same rationale — TouhouDB tags can change over time)
    conn.execute(song_tags.delete().where(song_tags.c.song_id == song_id))
    _upsert_song_tags(detail.tags, song_id, conn)

    logger.debug("Upserted song id=%d touhoudb_id=%d %r", song_id, detail.id, detail.name)
    return song_id


def _upsert_song_character(
    credit: ArtistForSong,
    song_id: int,
    conn: Connection,
) -> None:
    """
    Upsert a Touhou character into ``characters`` and link it to the song via
    ``song_characters``.

    TouhouDB credits characters (e.g. Yoshika Miyako, Seiga Kaku) as artists
    with ``artistType: "Character"``.  They are subjects of the song, not
    producers, so they belong in ``song_characters`` rather than
    ``song_artists``.

    Uses ``touhoudb_id`` as the upsert key; ``name_romanized`` is taken from
    ``additionalNames`` (TouhouDB puts the romanized name there).
    """
    artist = credit.artist
    if artist is None:
        return

    # additionalNames is a comma-separated list; the first entry is the
    # romanized name (e.g. "Yoshika Miyako") and the rest are alternate names.
    additional = [n.strip() for n in artist.additionalNames.split(",") if n.strip()]
    name_romanized = additional[0] if additional else None
    other_names = additional[1:] if len(additional) > 1 else None

    stmt = (
        pg_insert(characters)
        .values(
            touhoudb_id=artist.id,
            name=artist.name,
            name_romanized=name_romanized,
            other_names=other_names,
        )
        .on_conflict_do_update(
            index_elements=["touhoudb_id"],
            set_={
                "name": artist.name,
                "name_romanized": name_romanized,
                "other_names": other_names,
            },
        )
        .returning(characters.c.id)
    )
    character_id: int = conn.execute(stmt).scalar_one()

    conn.execute(
        pg_insert(song_characters)
        .values(song_id=song_id, character_id=character_id)
        .on_conflict_do_nothing()
    )
    logger.debug("Linked character %r (touhoudb_id=%d) to song %d", artist.name, artist.id, song_id)


def _upsert_song_artists(
    credits: list[ArtistForSong],
    song_id: int,
    conn: Connection,
) -> None:
    for credit in credits:
        if credit.isSupport:
            continue  # skip support/featuring credits for now

        # Characters (e.g. Yoshika Miyako, Seiga) are Touhou game characters
        # credited by TouhouDB as "subjects" of the song, not actual producers.
        # Route them to song_characters instead of song_artists to keep
        # arranger/vocalist analytics clean.
        if credit.artist and credit.artist.artistType.lower() == "character":
            _upsert_song_character(credit, song_id, conn)
            continue

        artist_id = _upsert_artist(credit, conn)
        if artist_id is None:
            continue

        raw_artist_type = credit.artist.artistType if credit.artist else "Unknown"
        logger.debug(
            "Artist %r (type=%r) roles=%r effectiveRoles=%r",
            credit.artist.name if credit.artist else "?",
            raw_artist_type,
            credit.roles,
            credit.effectiveRoles,
        )

        for raw_role in credit.role_list:
            role = _map_role(raw_role, artist_type=raw_artist_type)
            if role is None:
                logger.debug("Skipping unmapped role %r for artist %d", raw_role, artist_id)
                continue
            conn.execute(
                pg_insert(song_artists)
                .values(song_id=song_id, artist_id=artist_id, role=role)
                .on_conflict_do_nothing()
            )


def _upsert_song_tags(tag_votes: list[Any], song_id: int, conn: Connection) -> None:
    for tv in tag_votes:
        conn.execute(
            pg_insert(song_tags)
            .values(song_id=song_id, tag=tv.tag.name.lower(), count=tv.count)
            .on_conflict_do_update(
                index_elements=["song_id", "tag"],
                set_={"count": tv.count},
            )
        )


# ---------------------------------------------------------------------------
# Album mapper
# ---------------------------------------------------------------------------


def map_album_to_db(detail: AlbumDetail, conn: Connection) -> int:
    """
    Upsert an album and its related rows from an ``AlbumDetail`` response.

    Touches: ``albums``, ``artists``, ``album_circles``, ``album_events``,
    ``album_tags``.

    Does NOT upsert ``album_tracks`` — that requires the per-track songs to
    already exist in the DB, which is handled by the pipeline (it calls
    ``map_song_to_db`` for each track song first, then calls
    ``link_album_tracks``).

    Returns the internal ``albums.id``.
    """
    disc_type = _map_disc_type(detail.discType)
    release_date = detail.releaseDate.to_date() if detail.releaseDate else None

    stmt = (
        pg_insert(albums)
        .values(
            touhoudb_id=detail.id,
            title=detail.name,
            release_date=release_date,
            catalog_number=detail.catalogNumber,
            barcode=detail.barcode,
            description=detail.description,
            disc_type=disc_type,
            touhoudb_url=f"https://touhoudb.com/Al/{detail.id}",
        )
        .on_conflict_do_update(
            index_elements=["touhoudb_id"],
            set_={
                "title": detail.name,
                "release_date": release_date,
                "catalog_number": detail.catalogNumber,
                "barcode": detail.barcode,
                "description": detail.description,
                "disc_type": disc_type,
            },
        )
        .returning(albums.c.id)
    )
    album_id: int = conn.execute(stmt).scalar_one()

    # Upsert circle credits
    _upsert_album_circles(detail.artists, album_id, conn)

    # Upsert events from artist list (TouhouDB stores event names as artists of
    # type "Label" with special naming — but that's complex; skip for M2)

    # Upsert tags
    for tv in detail.tags:
        conn.execute(
            pg_insert(album_tags)
            .values(album_id=album_id, tag=tv.tag.name.lower(), count=tv.count)
            .on_conflict_do_update(
                index_elements=["album_id", "tag"],
                set_={"count": tv.count},
            )
        )

    logger.debug("Upserted album id=%d touhoudb_id=%d %r", album_id, detail.id, detail.name)
    return album_id


def _upsert_album_circles(
    credits: list[ArtistForAlbum],
    album_id: int,
    conn: Connection,
) -> None:
    for credit in credits:
        artist_id = _upsert_artist(credit, conn)
        if artist_id is None:
            continue
        is_primary = "Default" in credit.role_list or len(credit.role_list) == 0
        conn.execute(
            pg_insert(album_circles)
            .values(album_id=album_id, circle_id=artist_id, is_primary=is_primary)
            .on_conflict_do_update(
                index_elements=["album_id", "circle_id"],
                set_={"is_primary": is_primary},
            )
        )


# ---------------------------------------------------------------------------
# Original-song linker (called by pipeline after resolve_original_chain)
# ---------------------------------------------------------------------------


def link_song_originals(
    song_id: int,
    original_touhoudb_ids: list[int],
    conn: Connection,
) -> list[int]:
    """
    For each TouhouDB original song ID in ``original_touhoudb_ids``, find the
    matching row in ``original_songs`` (keyed by ``touhoudb_id``) and insert
    a ``song_originals`` row.

    Returns the list of ``original_songs.id`` values that were successfully
    linked.  IDs that have no matching ``original_songs`` row are silently
    skipped here — the caller should surface a task.

    KNOWN LIMITATION: The seeded ``original_songs`` rows have
    ``touhoudb_id = NULL`` (they were seeded from game names, not TouhouDB).
    Until ``touhoudb_id`` is backfilled, every call to this function returns
    an empty list and the pipeline creates a FILL_MISSING_INFO task for every
    matched song.

    TODO (M5 — original song backfill): Add a one-time migration step that:
    1. Fetches each original song from TouhouDB by name + work title.
    2. Fuzzy-matches against our seeded ``original_songs`` rows.
    3. Sets ``original_songs.touhoudb_id`` for confirmed matches.
    After that step, this function will start returning non-empty lists and
    ``song_originals`` will be populated correctly.
    """
    from lotad.db.models import song_originals  # avoid circular at module level

    linked: list[int] = []
    for tdb_id in original_touhoudb_ids:
        row = conn.execute(
            sa.select(original_songs.c.id).where(original_songs.c.touhoudb_id == tdb_id)
        ).one_or_none()
        if row is None:
            logger.debug("original_songs has no row with touhoudb_id=%d — skipping link", tdb_id)
            continue
        conn.execute(
            pg_insert(song_originals)
            .values(song_id=song_id, original_song_id=row.id)
            .on_conflict_do_nothing()
        )
        linked.append(row.id)
    return linked
