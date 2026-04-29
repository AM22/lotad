"""Interactive ingest actions and classification editor for the resolve wizard."""

from __future__ import annotations

import logging
from typing import Any

import click
import sqlalchemy as sa

from lotad.agents.llm_extractor import VideoClassification, VideoType
from lotad.cli.tasks._shared import console
from lotad.config import get_settings
from lotad.db.models import playlist_songs as ps_table
from lotad.db.models import songs as songs_table
from lotad.db.models import youtube_videos as yt_table
from lotad.db.session import get_engine
from lotad.ingestion.mappers import ingest_song_from_llm_classification
from lotad.ingestion.pipeline import IngestPipeline
from lotad.ingestion.youtube_client import PlaylistItem
from lotad.tasks import manager

logger = logging.getLogger(__name__)

_CLS_STRING_FIELDS: list[tuple[str, str]] = [
    ("song_title", "Song title"),
    ("circle_name", "Circle"),
    ("album_title", "Album"),
    ("release_date", "Release date"),
    ("release_event", "Event"),
    ("original_game_name", "Source game"),
]
_CLS_LIST_FIELDS: list[tuple[str, str]] = [
    ("arranger_names", "Arrangers"),
    ("vocalist_names", "Vocalists"),
    ("lyricist_names", "Lyricists"),
    ("original_song_names", "Original songs"),
]


def _print_classification_summary(cls: dict[str, Any]) -> None:
    """Print a compact classification panel to the console."""
    lines: list[str] = []
    for key, label in _CLS_STRING_FIELDS:
        val = cls.get(key)
        if val:
            lines.append(f"  {label:<16}: {val}")
    for key, label in _CLS_LIST_FIELDS:
        vals = cls.get(key) or []
        if vals:
            lines.append(f"  {label:<16}: {', '.join(vals)}")
    is_orig = cls.get("is_original_composition")
    if is_orig is not None:
        lines.append(f"  {'Is original':<16}: {is_orig}")
    vtype = cls.get("video_type", "?")
    conf = cls.get("confidence_in_classification", "?")
    lines.append(f"  {'Video type':<16}: {vtype}  (confidence: {conf})")
    if cls.get("extraction_notes"):
        lines.append(f"  {'Notes':<16}: {cls['extraction_notes']}")
    console.print("\n".join(lines) if lines else "  (no fields extracted)")


def _prompt_classification_overrides(cls: dict[str, Any]) -> dict[str, Any]:
    """
    Interactive field-by-field editor for an LLM classification dict.

    Each field is shown with its current value; pressing Enter keeps it.
    Lists are entered as comma-separated strings.  Returns the edited dict.
    """
    result = dict(cls)

    console.print(
        "\n[bold]Edit fields[/bold] [dim](Enter = keep current value, type to override)[/dim]"
    )

    for key, label in _CLS_STRING_FIELDS:
        current = result.get(key) or ""
        hint = " (YYYY-MM-DD)" if key == "release_date" else ""
        # Show current value in brackets so the user can see it without retyping
        display = f"[{current}]" if current else "[empty]"
        console.print(f"  {label}{hint} {display}")
        new_val = click.prompt(f"  → {label}{hint}", default=current, show_default=False)
        result[key] = new_val.strip() or None

    for key, label in _CLS_LIST_FIELDS:
        current_list = result.get(key) or []
        current_str = ", ".join(current_list)
        display = f"[{current_str}]" if current_str else "[empty]"
        console.print(f"  {label} {display}")
        new_str = click.prompt(
            f"  → {label} (comma-separated)", default=current_str, show_default=False
        )
        result[key] = [x.strip() for x in new_str.split(",") if x.strip()]

    # is_original_composition: three-state bool
    is_orig = result.get("is_original_composition")
    current_bool = "y" if is_orig is True else ("n" if is_orig is False else "?")
    console.print(f"  Is original composition [{current_bool}]")
    raw = click.prompt("  → Is original composition? (y/n/?)", default=current_bool)
    raw = raw.strip().lower()
    result["is_original_composition"] = True if raw == "y" else (False if raw == "n" else None)

    console.print()
    console.print("[bold]Edited classification:[/bold]")
    _print_classification_summary(result)
    console.print()

    return result


def _prompt_timestamp_mode(
    count: int,
    *,
    hint_timestamps: list[int | None] | None = None,
) -> list[int | None] | None:
    """
    Interactively ask the user how to assign timestamps for composite ingest.

    Returns:
      - ``None``          → auto-cumulative mode (derive from TouhouDB durations)
      - list of int|None  → explicit per-track timestamps in seconds
    """
    console.print()
    console.print(f"[bold]Timestamp assignment for {count} track(s):[/bold]")

    has_hints = bool(hint_timestamps and any(t is not None for t in hint_timestamps))
    if has_hints:
        assert hint_timestamps is not None
        hints_str = ", ".join(
            (f"{t // 60}:{t % 60:02d}" if t is not None else "?") for t in hint_timestamps
        )
        console.print(f"  LLM timestamps : {hints_str}")
        console.print("[L] Use LLM timestamps (above)")

    console.print("[A] Auto — cumulative from TouhouDB song durations (default for albums)")
    console.print("[M] Manual — enter timestamps in seconds, comma-separated")
    console.print("[Z] All zero — set every track to 0:00 (mashups / full-video crossfades)")

    default = "L" if has_hints else "A"
    choice = click.prompt("Timestamp mode", default=default).strip().upper()

    if choice == "L" and has_hints:
        return hint_timestamps
    if choice == "M":
        raw = click.prompt(f"Enter {count} timestamp(s) in seconds (comma-separated)")
        ts_list: list[int | None] = []
        for part in raw.split(","):
            part = part.strip()
            try:
                ts_list.append(int(part))
            except ValueError:
                ts_list.append(None)
        # Pad / trim to exactly `count` entries
        while len(ts_list) < count:
            ts_list.append(None)
        return ts_list[:count]
    if choice == "Z":
        return [0] * count
    # "A" or fallback — signal _do_ingest_composite to compute cumulatively
    return None


def _prompt_video_type_override(current: str) -> VideoType:
    console.print(f"\nCurrent video type: [bold]{current}[/bold]")
    console.print("[1] single_song")
    console.print("[2] composite_tracks")
    console.print("[3] full_album")
    choice = click.prompt("New type", default="1").strip()
    return {
        "1": VideoType.SINGLE_SONG,
        "2": VideoType.COMPOSITE_TRACKS,
        "3": VideoType.FULL_ALBUM,
    }.get(choice, VideoType.SINGLE_SONG)


async def _do_ingest_single(
    task_id: int, data: dict[str, Any], video_row: Any, touhoudb_id: int
) -> None:
    """Ingest a single TouhouDB song into a playlist and resolve the task."""
    playlist_db_id = data.get("playlist_db_id")
    if playlist_db_id is None:
        console.print(
            "[red]Error: task data missing playlist_db_id. Cannot re-ingest automatically.[/red]"
        )
        return

    if video_row is None:
        console.print("[red]Error: no linked youtube_videos row found.[/red]")
        return

    console.print(f"\nIngesting TouhouDB #{touhoudb_id} for {video_row['video_id']!r}...")

    item = PlaylistItem(
        video_id=video_row["video_id"],
        title=video_row.get("title") or "",
        description=video_row.get("description") or "",
        channel_name=video_row.get("channel_name") or "",
        duration_seconds=video_row.get("duration_seconds"),
        position=0,
        is_available=True,
    )

    settings = get_settings()
    async with IngestPipeline(settings) as pipeline:
        ok = await pipeline.ingest_video(
            item,
            playlist_db_id=playlist_db_id,
            bulk_match={item.video_id: touhoudb_id},
        )

    if ok:
        engine = get_engine()
        with engine.begin() as conn:
            yt_db_id = conn.execute(
                sa.select(yt_table.c.id).where(yt_table.c.video_id == item.video_id)
            ).scalar_one_or_none()
            song_id = None
            if yt_db_id:
                song_id = conn.execute(
                    sa.select(ps_table.c.song_id)
                    .where(
                        sa.and_(
                            ps_table.c.youtube_video_id == yt_db_id,
                            ps_table.c.removed_at.is_(None),
                        )
                    )
                    .limit(1)
                ).scalar_one_or_none()
            manager.resolve_ingest_failed(conn, task_id, song_id=song_id or touhoudb_id)
        console.print(f"[green]✓ Ingested successfully. Task #{task_id} resolved.[/green]")
    else:
        console.print(
            f"[yellow]Pipeline returned no match for TouhouDB #{touhoudb_id}. "
            f"Task remains open.[/yellow]"
        )


async def _do_ingest_composite(
    task_id: int,
    data: dict[str, Any],
    video_row: Any,
    touhoudb_ids: list[int],
    *,
    video_type: VideoType,
    hint_timestamps: list[int | None] | None = None,
) -> None:
    """Ingest multiple TouhouDB songs from a composite video and resolve the task."""
    playlist_db_id = data.get("playlist_db_id")
    if playlist_db_id is None or video_row is None:
        console.print("[red]Error: missing playlist_db_id or video row.[/red]")
        return

    console.print(f"\nIngesting {len(touhoudb_ids)} tracks from composite video...")
    settings = get_settings()
    success_count = 0

    # Timestamps are computed incrementally: for each track, the timestamp is
    # either the LLM hint (if provided) or the running cumulative offset.  We
    # can only query a song's duration *after* ingest_video has upserted it,
    # so cursor advancement happens inside the loop after each successful call.
    # A single connection is reused for all duration lookups (ingest_video
    # manages its own connection internally).
    cursor: int | None = 0

    with get_engine().connect() as dur_conn:
        async with IngestPipeline(settings) as pipeline:
            for i, tdb_id in enumerate(touhoudb_ids):
                hint = (
                    hint_timestamps[i] if (hint_timestamps and i < len(hint_timestamps)) else None
                )
                ts = hint if hint is not None else cursor

                item = PlaylistItem(
                    video_id=video_row["video_id"],
                    title=video_row.get("title") or "",
                    description=video_row.get("description") or "",
                    channel_name=video_row.get("channel_name") or "",
                    duration_seconds=video_row.get("duration_seconds"),
                    position=0,
                    is_available=True,
                )
                ok = await pipeline.ingest_video(
                    item,
                    playlist_db_id=playlist_db_id,
                    bulk_match={item.video_id: tdb_id},
                    video_type_hint=video_type,
                    youtube_timestamp_seconds=ts,
                )
                status_str = "[green]✓[/green]" if ok else "[red]✗[/red]"
                ts_str = f" @ {ts // 60}:{ts % 60:02d}" if ts is not None else ""
                console.print(f"  {status_str} TouhouDB #{tdb_id}{ts_str}")
                if ok:
                    success_count += 1
                    # Advance cursor using the song's duration now that it's in our DB
                    dur = dur_conn.execute(
                        sa.select(songs_table.c.duration_seconds).where(
                            songs_table.c.touhoudb_id == tdb_id
                        )
                    ).scalar_one_or_none()
                    if ts is not None and dur is not None:
                        cursor = ts + dur
                    else:
                        if dur is None:
                            logger.warning(
                                "No duration for TouhouDB #%d; timestamp cursor reset to None",
                                tdb_id,
                            )
                        cursor = None

    if success_count > 0:
        with get_engine().begin() as conn:
            manager.resolve_ingest_failed(conn, task_id, song_id=0)
        console.print(
            f"[green]Ingested {success_count}/{len(touhoudb_ids)} tracks. "
            f"Task #{task_id} resolved.[/green]"
        )
    else:
        console.print("[yellow]No tracks ingested successfully. Task remains open.[/yellow]")


async def _do_ingest_stub(
    task_id: int, data: dict[str, Any], video_row: Any, llm_cls: dict[str, Any]
) -> None:
    """Insert a stub song from LLM classification (no TouhouDB linkage) and resolve the task."""
    playlist_db_id = data.get("playlist_db_id")
    if playlist_db_id is None or video_row is None:
        console.print("[red]Error: missing playlist_db_id or video row.[/red]")
        return

    try:
        classification = VideoClassification.model_validate(llm_cls)
    except Exception as exc:
        console.print(f"[red]Could not parse LLM classification: {exc}[/red]")
        return

    engine = get_engine()
    with engine.begin() as conn:
        yt_db_id = conn.execute(
            sa.select(yt_table.c.id).where(yt_table.c.video_id == video_row["video_id"])
        ).scalar_one_or_none()

        if yt_db_id is None:
            console.print("[red]Could not find youtube_videos.id for this video.[/red]")
            return

        song_id = ingest_song_from_llm_classification(
            classification,
            playlist_db_id,
            yt_db_id,
            conn,
            duration_seconds=video_row.get("duration_seconds"),
        )
        manager.resolve_ingest_failed(conn, task_id, song_id=song_id)

    console.print(f"[green]✓ Inserted stub song (id={song_id}). Task #{task_id} resolved.[/green]")
