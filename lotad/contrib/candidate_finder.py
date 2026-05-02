"""Find LOTAD songs whose TouhouDB entry could gain a YouTube PV.

The query side selects songs that:
  * already have a ``touhoudb_id`` (so we know which entry to edit), and
  * have at least one *individual* (single-song) YouTube video linked via an
    active ``playlist_songs`` row, with a known, available YouTube video id.

We deliberately do **not** filter on "TouhouDB entry is missing a Youtube PV"
here — that requires a fresh `GET /api/songs/{id}` round-trip per candidate,
which the writer performs at submit time (avoids racing with manual edits and
keeps the candidate query pure SQL).

For each candidate we also classify the suggested ``PVType`` ("Original" vs
"Reprint") via a cheap channel-name substring match against the song's known
artists / circles in ``song_artists``. No extra HTTP traffic, no LLM call.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa
from sqlalchemy.engine import Connection

from lotad.db.models import (
    artists,
    playlist_songs,
    song_artists,
    songs,
    youtube_videos,
)


@dataclass(frozen=True)
class VideoLinkCandidate:
    """One song eligible for a TouhouDB YouTube PV draft submission."""

    song_id: int
    touhoudb_id: int
    song_title: str
    video_id: str
    video_url: str
    channel_name: str | None
    channel_id: str | None
    video_title: str | None
    video_duration_seconds: int | None
    suggested_pv_type: str  # "Original" or "Reprint"


def find_candidates(
    conn: Connection,
    *,
    limit: int | None = None,
) -> list[VideoLinkCandidate]:
    """Return candidate songs, newest playlist additions first."""
    # Pick the most recently added active playlist_songs row per song so that
    # if a song lives in multiple playlists with different uploads, we surface
    # the freshest one (most likely to still be live on YouTube).
    rn_subq = (
        sa.select(
            playlist_songs.c.song_id,
            playlist_songs.c.youtube_video_id,
            playlist_songs.c.added_at,
            sa.func.row_number()
            .over(
                partition_by=playlist_songs.c.song_id,
                order_by=playlist_songs.c.added_at.desc(),
            )
            .label("rn"),
        )
        .where(
            playlist_songs.c.removed_at.is_(None),
            playlist_songs.c.source_type == "INDIVIDUAL_VIDEO",
            playlist_songs.c.youtube_video_id.is_not(None),
        )
        .subquery()
    )

    stmt = (
        sa.select(
            songs.c.id.label("song_id"),
            songs.c.touhoudb_id,
            songs.c.title.label("song_title"),
            youtube_videos.c.video_id,
            youtube_videos.c.title.label("video_title"),
            youtube_videos.c.channel_id,
            youtube_videos.c.channel_name,
            youtube_videos.c.duration_seconds,
        )
        .select_from(
            songs.join(rn_subq, rn_subq.c.song_id == songs.c.id).join(
                youtube_videos, youtube_videos.c.id == rn_subq.c.youtube_video_id
            )
        )
        .where(
            songs.c.touhoudb_id.is_not(None),
            youtube_videos.c.is_available.is_(True),
            rn_subq.c.rn == 1,
        )
        .order_by(rn_subq.c.added_at.desc())
    )
    if limit is not None:
        stmt = stmt.limit(limit)

    rows = conn.execute(stmt).mappings().all()
    if not rows:
        return []

    song_ids = [r["song_id"] for r in rows]
    artist_index = _load_artist_names_by_song(conn, song_ids)

    out: list[VideoLinkCandidate] = []
    for r in rows:
        pv_type = _classify_pv_type(r["channel_name"], artist_index.get(r["song_id"], []))
        out.append(
            VideoLinkCandidate(
                song_id=r["song_id"],
                touhoudb_id=r["touhoudb_id"],
                song_title=r["song_title"],
                video_id=r["video_id"],
                video_url=f"https://youtu.be/{r['video_id']}",
                channel_name=r["channel_name"],
                channel_id=r["channel_id"],
                video_title=r["video_title"],
                video_duration_seconds=r["duration_seconds"],
                suggested_pv_type=pv_type,
            )
        )
    return out


def _load_artist_names_by_song(conn: Connection, song_ids: list[int]) -> dict[int, list[str]]:
    """Return song_id -> [artist names + romanizations] for all roles."""
    if not song_ids:
        return {}
    stmt = (
        sa.select(song_artists.c.song_id, artists.c.name, artists.c.name_romanized)
        .select_from(song_artists.join(artists, artists.c.id == song_artists.c.artist_id))
        .where(song_artists.c.song_id.in_(song_ids))
    )
    out: dict[int, list[str]] = {}
    for row in conn.execute(stmt).mappings():
        bucket = out.setdefault(row["song_id"], [])
        if row["name"]:
            bucket.append(row["name"])
        if row["name_romanized"]:
            bucket.append(row["name_romanized"])
    return out


def _classify_pv_type(channel_name: str | None, artist_names: list[str]) -> str:
    """Return "Original" if the YT channel looks like the song's artist, else "Reprint".

    We don't have a reliable signal for distinguishing the artist's own upload
    from a re-upload, so we use a substring match in either direction (channel
    contains artist, or artist contains channel — covers cases where the
    channel adds a suffix like "Records" or "Official"). Conservative default
    is Reprint, since flipping a misclassified Reprint to Original on review is
    less embarrassing than the opposite.
    """
    if not channel_name:
        return "Reprint"
    chan = channel_name.casefold().strip()
    if not chan:
        return "Reprint"
    for raw in artist_names:
        if not raw:
            continue
        artist = raw.casefold().strip()
        # Skip very short artist names (single character) — too many false positives.
        if len(artist) < 2:
            continue
        if artist in chan or chan in artist:
            return "Original"
    return "Reprint"
