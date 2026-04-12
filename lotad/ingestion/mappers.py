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

import difflib
import logging
from datetime import date
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Connection
from sqlalchemy.dialects.postgresql import insert as pg_insert

from lotad.db.models import (
    ArtistType,
    ConfidenceLevel,
    DiscType,
    MediaType,
    SongRole,
    SongType,
    album_circles,
    album_events,
    album_tags,
    album_tracks,
    albums,
    artists,
    characters,
    original_song_characters,
    original_songs,
    song_artists,
    song_characters,
    song_tags,
    songs,
    works,
)
from lotad.ingestion.touhoudb_models import (
    AlbumDetail,
    AlbumSummary,
    ArtistForAlbum,
    ArtistForSong,
    SongDetail,
)

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
    additional = [n.strip() for n in artist_summary.additionalNames.split(",") if n.strip()]
    name_romanized = additional[0] if additional else None
    stmt = (
        pg_insert(artists)
        .values(
            touhoudb_id=artist_summary.id,
            name=artist_summary.name,
            name_romanized=name_romanized,
            artist_type=artist_type,
            touhoudb_url=f"https://touhoudb.com/Ar/{artist_summary.id}",
        )
        .on_conflict_do_update(
            index_elements=["touhoudb_id"],
            set_={
                "name": artist_summary.name,
                "name_romanized": name_romanized,
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
    additional = [n.strip() for n in detail.additionalNames.split(",") if n.strip()]
    title_romanized = additional[0] if additional else None

    # 1. Upsert the song row
    stmt = (
        pg_insert(songs)
        .values(
            touhoudb_id=detail.id,
            title=detail.name,
            title_romanized=title_romanized,
            duration_seconds=detail.lengthSeconds,
            has_lyrics=has_lyrics,
            is_original_composition=is_orig,
            song_type=song_type,
            publish_date=_parse_publish_date(detail.publishDate),
            notes=detail.notes,
            touhoudb_url=f"https://touhoudb.com/S/{detail.id}",
        )
        .on_conflict_do_update(
            index_elements=["touhoudb_id"],
            set_={
                "title": detail.name,
                "title_romanized": title_romanized,
                "duration_seconds": detail.lengthSeconds,
                "has_lyrics": has_lyrics,
                "is_original_composition": is_orig,
                "song_type": song_type,
                "publish_date": _parse_publish_date(detail.publishDate),
                "notes": detail.notes,
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
    additional = [n.strip() for n in detail.additionalNames.split(",") if n.strip()]
    title_romanized = additional[0] if additional else None

    stmt = (
        pg_insert(albums)
        .values(
            touhoudb_id=detail.id,
            title=detail.name,
            title_romanized=title_romanized,
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
                "title_romanized": title_romanized,
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

    # Upsert release events (top-level list returned when ReleaseEvent is in fields)
    for event in detail.releaseEvents:
        conn.execute(
            pg_insert(album_events)
            .values(album_id=album_id, event_name=event.name, touhoudb_id=event.id)
            .on_conflict_do_nothing()
        )

    logger.debug("Upserted album id=%d touhoudb_id=%d %r", album_id, detail.id, detail.name)
    return album_id


def link_album_tracks(album_id: int, detail: AlbumDetail, conn: Connection) -> int:
    """
    Link tracks of an album to songs already in the DB.

    For each track in ``AlbumDetail.tracks`` whose song already has a row in
    ``songs`` (keyed on ``touhoudb_id``), inserts an ``album_tracks`` row.
    Tracks whose songs haven't been ingested yet are silently skipped —
    they will be linked when those songs are ingested later.

    Returns the number of tracks linked.
    """
    linked = 0
    for track in detail.tracks:
        if track.song is None or track.trackNumber is None:
            continue
        row = conn.execute(
            sa.select(songs.c.id).where(songs.c.touhoudb_id == track.song.id)
        ).one_or_none()
        if row is None:
            continue
        conn.execute(
            pg_insert(album_tracks)
            .values(
                album_id=album_id,
                song_id=row.id,
                track_number=track.trackNumber,
                disc_number=track.discNumber,
            )
            .on_conflict_do_nothing()
        )
        linked += 1
    return linked


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

    KNOWN LIMITATION: The seeded ``original_songs`` rows previously had
    ``touhoudb_id = NULL``.  Once ``lotad originals scrape`` has been run,
    ``touhoudb_id`` is populated and this function will correctly link songs.
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


# ---------------------------------------------------------------------------
# Original-song scraper helpers (called by `lotad originals scrape`)
# ---------------------------------------------------------------------------

# Maps TouhouDB album discType (lowercase) to our works.media_type for year-based
# work matching.  Only types with a clear mapping are included; unknown types fall
# through to the difflib name-match strategy.
_DISC_TYPE_TO_MEDIA_TYPE: dict[str, MediaType] = {
    "game": MediaType.GAME,
    # ZUN's standalone music CDs (Ghostly Field Club series, etc.) appear as
    # "Album" or "Other" disc types on TouhouDB.
    "album": MediaType.MUSIC_CD,
    "single": MediaType.MUSIC_CD,
    "ep": MediaType.MUSIC_CD,
    "other": MediaType.MUSIC_CD,
}

# Maps TouhouDB tag urlSlug values to stage integers.
# 0=title theme, 1-6=stage N, 7=extra stage, 8=ending theme, 9=staff roll.
_STAGE_SLUG_MAP: dict[str, int] = {
    "title-theme": 0,
    "first-stage": 1,
    "second-stage": 2,
    "third-stage": 3,
    "fourth-stage": 4,
    "fifth-stage": 5,
    "sixth-stage": 6,
    "extra-stage": 7,
    "ending-theme": 8,
    "staff-roll": 9,
}


def _parse_stage_from_tags(tags: list[Any]) -> int | None:
    """Derive stage integer from a song's TouhouDB tag list."""
    for tv in tags:
        slug = tv.tag.urlSlug.lower() if tv.tag.urlSlug else ""
        stage = _STAGE_SLUG_MAP.get(slug)
        if stage is not None:
            return stage
    return None


def match_work_for_song(albums: list[AlbumSummary], conn: Connection) -> int | None:
    """
    Find the best matching ``works.id`` for an original song given its album list.
    Writes back the matched TouhouDB album ID to ``works.touhoudb_id`` on first match.

    Strategy:
    0. Fast path: if ``works.touhoudb_id`` already matches any album in the list,
       return that work immediately (no heuristics needed on re-runs).
    1. Sort the song's albums by release date (oldest first).
    2. Map the oldest album's ``discType`` to our ``media_type`` via
       ``_DISC_TYPE_TO_MEDIA_TYPE``.  If a mapping exists and we have a release
       year, filter works by **both** ``media_type`` and ``release_year``.
       This prevents cross-type collisions (e.g. a music CD year matching a game).
       - Exactly 1 match → done.
       - Multiple matches → name-similarity tiebreak (rare for music CDs; can
         happen for games with two releases in the same year).
    3. Fall back to difflib string match on ``works.name`` vs album name across
       all works (threshold ≥ 0.6).
    4. Return None if no confident match is found (caller logs and skips).

    When a match is found via heuristics (strategies 1–3), writes the TouhouDB
    album ID back to ``works.touhoudb_id`` so future calls use the fast path.
    """
    if not albums:
        return None

    album_ids = {a.id for a in albums}

    # Fetch all works once (including touhoudb_id for fast-path lookup)
    all_works = conn.execute(
        sa.select(
            works.c.id, works.c.name, works.c.release_year, works.c.media_type, works.c.touhoudb_id
        )
    ).fetchall()
    if not all_works:
        return None

    # Strategy 0: fast path — touhoudb_id already stored on a work
    for w in all_works:
        if w.touhoudb_id and w.touhoudb_id in album_ids:
            return w.id

    # Sort albums by release year ascending, picking the oldest
    def _sort_key(a: AlbumSummary) -> int:
        if a.releaseDate and a.releaseDate.year:
            return a.releaseDate.year
        return 9999

    sorted_albums = sorted(albums, key=_sort_key)
    oldest = sorted_albums[0]

    release_year = oldest.releaseDate.year if oldest.releaseDate else None
    album_name = oldest.defaultName or oldest.name

    matched_work_id: int | None = None

    # Strategy 1: year + media_type match (type-safe — no cross-type collisions)
    expected_media_type = _DISC_TYPE_TO_MEDIA_TYPE.get(oldest.discType.lower())
    if expected_media_type and release_year:
        year_matches = [
            w
            for w in all_works
            if w.media_type == expected_media_type and w.release_year == release_year
        ]
        if len(year_matches) == 1:
            matched_work_id = year_matches[0].id
        elif len(year_matches) > 1 and album_name:
            # Tie-break by name similarity (e.g. two games in the same year)
            logger.warning(
                "Multiple %s works for year=%d — using name similarity tiebreak",
                expected_media_type,
                release_year,
            )
            best = max(
                year_matches,
                key=lambda w: difflib.SequenceMatcher(
                    None, w.name.lower(), album_name.lower()
                ).ratio(),
            )
            matched_work_id = best.id

    # Strategy 2: difflib name match across all works
    if matched_work_id is None and album_name:
        best_ratio = 0.0
        best_work_id: int | None = None
        for w in all_works:
            ratio = difflib.SequenceMatcher(None, w.name.lower(), album_name.lower()).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_work_id = w.id

        if best_ratio >= 0.6:
            matched_work_id = best_work_id
        else:
            logger.warning(
                "Could not confidently match album %r (year=%s, discType=%s) to any work "
                "(best ratio=%.2f)",
                album_name,
                release_year,
                oldest.discType,
                best_ratio,
            )

    # Write back the TouhouDB album ID so future runs skip heuristics
    if matched_work_id is not None:
        conn.execute(
            works.update()
            .where(works.c.id == matched_work_id)
            .where(works.c.touhoudb_id.is_(None))  # don't overwrite an existing mapping
            .values(touhoudb_id=oldest.id)
        )

    return matched_work_id


def upsert_original_song(detail: SongDetail, work_id: int | None, conn: Connection) -> int:
    """
    Insert or update a row in ``original_songs`` from a TouhouDB ``SongDetail``.

    Keyed on ``touhoudb_id``.  Stage is derived from the song's tags.
    ``is_boss`` defaults to False — TODO: backfill from TouhouWiki.

    ``work_id`` may be None for songs whose source work is not (yet) seeded —
    e.g. unused tracks, non-Touhou ZUN games without a work row.

    Returns the internal ``original_songs.id``.
    """
    additional = [n.strip() for n in detail.additionalNames.split(",") if n.strip()]
    name_romanized = additional[0] if additional else None
    stage = _parse_stage_from_tags(detail.tags)
    # SongNotes is a structured object; flatten to plain text for DB storage.
    notes_text = (detail.notes.all_text() or None) if detail.notes else None

    stmt = (
        pg_insert(original_songs)
        .values(
            touhoudb_id=detail.id,
            name=detail.name,
            name_romanized=name_romanized,
            work_id=work_id,
            stage=stage,
            is_boss=False,
            min_milli_bpm=detail.minMilliBpm,
            max_milli_bpm=detail.maxMilliBpm,
            notes=notes_text,
        )
        .on_conflict_do_update(
            index_elements=["touhoudb_id"],
            set_={
                "name": detail.name,
                "name_romanized": name_romanized,
                "work_id": work_id,
                "stage": stage,
                "min_milli_bpm": detail.minMilliBpm,
                "max_milli_bpm": detail.maxMilliBpm,
                "notes": notes_text,
            },
        )
        .returning(original_songs.c.id)
    )
    original_song_id: int = conn.execute(stmt).scalar_one()
    logger.debug(
        "Upserted original_song id=%d touhoudb_id=%d %r stage=%s",
        original_song_id,
        detail.id,
        detail.name,
        stage,
    )
    return original_song_id


def link_original_song_characters(
    original_song_id: int,
    detail: SongDetail,
    conn: Connection,
) -> int:
    """
    Link Touhou characters to an original song via ``original_song_characters``.

    Characters are identified from the song's artist credits where
    ``artistType == "Character"``.  Each is upserted to the ``characters``
    table and linked with ``confidence = MEDIUM`` (50% — source data is
    reasonable but not verified).

    Returns the number of characters linked.
    """
    linked = 0
    for credit in detail.artists:
        artist = credit.artist
        if artist is None:
            continue
        if artist.artistType.lower() != "character":
            continue

        additional = [n.strip() for n in artist.additionalNames.split(",") if n.strip()]
        name_romanized = additional[0] if additional else None
        other_names = additional[1:] if len(additional) > 1 else None

        char_stmt = (
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
        character_id: int = conn.execute(char_stmt).scalar_one()

        conn.execute(
            pg_insert(original_song_characters)
            .values(
                original_song_id=original_song_id,
                character_id=character_id,
                confidence=ConfidenceLevel.MEDIUM,
            )
            .on_conflict_do_nothing()
        )
        logger.debug(
            "Linked character %r (touhoudb_id=%d) to original_song %d",
            artist.name,
            artist.id,
            original_song_id,
        )
        linked += 1

    return linked
