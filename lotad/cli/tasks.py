"""lotad tasks — task management commands."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Any

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from lotad.agents.llm_extractor import VideoType
from lotad.db.models import TaskStatus, TaskType
from lotad.db.session import get_engine
from lotad.tasks import manager

console = Console()
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TYPE_ORDER = [
    TaskType.INGEST_FAILED,
    TaskType.SUSPICIOUS_METADATA,
    TaskType.DEDUPLICATE_SONGS,
    TaskType.MISSING_LYRICIST,
    TaskType.DROPPED_VIDEO,
    TaskType.FILL_MISSING_INFO,
    TaskType.TOUHOUDB_UNREACHABLE,
    TaskType.REVIEW_ALBUM_TRACKS,
    TaskType.ASSIGN_PLAYLIST,
    TaskType.REVIEW_CHARACTER_MAPPING,
    TaskType.REVIEW_LOCAL_TRACK,
    TaskType.MISSING_CIRCLE,
]


def _age(created_at: Any) -> str:
    if created_at is None:
        return "?"
    if isinstance(created_at, str):
        try:
            created_at = datetime.fromisoformat(created_at)
        except ValueError:
            return "?"
    now = datetime.now(UTC)
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    delta = now - created_at
    if delta.days >= 1:
        return f"{delta.days}d ago"
    hours = delta.seconds // 3600
    if hours >= 1:
        return f"{hours}h ago"
    minutes = delta.seconds // 60
    return f"{minutes}m ago"


def _fmt_duration(seconds: int | None) -> str:
    if seconds is None:
        return "?"
    m, s = divmod(seconds, 60)
    return f"{m}:{s:02d}"


def _get_data(task_row: Any) -> dict:
    raw = task_row["data"]
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return {}
    return raw or {}


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group()
def tasks() -> None:
    """Manage human-in-the-loop review tasks."""


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@tasks.command("list")
@click.option("--type", "task_type", default=None, help="Filter by task type.")
@click.option(
    "--status",
    "status_str",
    default="OPEN",
    show_default=True,
    help="Filter by status (OPEN, RESOLVED, DISMISSED, IN_PROGRESS, all).",
)
@click.option("--limit", default=50, show_default=True, help="Max tasks shown per type.")
def tasks_list(task_type: str | None, status_str: str, limit: int) -> None:
    """List tasks grouped by type."""
    status: TaskStatus | None = None
    if status_str.lower() != "all":
        try:
            status = TaskStatus(status_str.upper())
        except ValueError:
            console.print(f"[red]Unknown status: {status_str!r}[/red]")
            raise click.Abort() from None

    parsed_type: TaskType | None = None
    if task_type:
        try:
            parsed_type = TaskType(task_type.upper())
        except ValueError:
            console.print(f"[red]Unknown task type: {task_type!r}[/red]")
            raise click.Abort() from None

    engine = get_engine()
    with engine.connect() as conn:
        counts = manager.count_tasks_by_type(conn, status=status)
        if not counts:
            console.print("[green]No tasks found.[/green]")
            return

        total = sum(counts.values())
        console.print(f"\n[bold]Tasks — {total} {status_str.lower()}[/bold]\n")

        type_list = [parsed_type] if parsed_type else _TYPE_ORDER
        for tt in type_list:
            count = counts.get(tt, 0)
            if count == 0:
                continue

            rows = manager.list_tasks(conn, task_type=tt, status=status, limit=limit)

            table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
            table.add_column("ID", style="cyan", width=6)
            table.add_column("Title", no_wrap=False, max_width=55)
            table.add_column("Age", width=9)
            if tt == TaskType.INGEST_FAILED:
                table.add_column("LLM", width=9)

            for row in rows:
                if tt == TaskType.INGEST_FAILED:
                    llm_cell = "[green]enriched[/green]" if row.get("llm_enriched_at") else "—"
                    table.add_row(str(row["id"]), row["title"], _age(row["created_at"]), llm_cell)
                else:
                    table.add_row(str(row["id"]), row["title"], _age(row["created_at"]))

            suffix = f" ({limit} of {count} shown)" if count > limit else ""
            title_str = f"[bold]{tt.value}[/bold] ({count}){suffix}"

            if tt == TaskType.INGEST_FAILED:
                unenriched = sum(1 for r in rows if not r.get("llm_enriched_at"))
                if unenriched > 0:
                    console.print(
                        Panel(
                            table,
                            title=title_str,
                            border_style="blue",
                            subtitle=f"[dim]{unenriched} unenriched — run `lotad tasks enrich --limit 100`[/dim]",
                        )
                    )
                    continue

            console.print(Panel(table, title=title_str, border_style="blue"))

        console.print()


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------


@tasks.command("show")
@click.argument("task_id", type=int)
def tasks_show(task_id: int) -> None:
    """Show full details for a task."""
    engine = get_engine()
    with engine.connect() as conn:
        ctx = manager.get_task_with_context(conn, task_id)

    if ctx is None:
        console.print(f"[red]Task #{task_id} not found.[/red]")
        raise click.Abort()

    task = ctx["task"]
    song = ctx["song"]
    video = ctx["video"]
    song_artists_rows = ctx["song_artists"]
    data = _get_data(task)

    status_color = {
        "OPEN": "yellow",
        "RESOLVED": "green",
        "DISMISSED": "dim",
        "IN_PROGRESS": "blue",
    }.get(str(task["status"]), "white")

    lines: list[str] = []
    lines.append(
        f"[bold]Task #{task['id']}[/bold] — [bold]{task['task_type']}[/bold]  "
        f"[{status_color}][{task['status']}][/{status_color}]  "
        f"created {_age(task['created_at'])}  priority={task['priority']}"
    )
    lines.append("")

    if video:
        yt_id = video.get("video_id", "")
        lines.append("[bold underline]Video[/bold underline]")
        lines.append(f"  YouTube ID  : {yt_id}")
        lines.append(f"  Title       : {video.get('title', '?')}")
        lines.append(f"  Channel     : {video.get('channel_name', '?')}")
        lines.append(f"  Duration    : {_fmt_duration(video.get('duration_seconds'))}")
        lines.append(f"  URL         : https://youtube.com/watch?v={yt_id}")
        lines.append("")

    if song:
        lines.append("[bold underline]Song[/bold underline]")
        lines.append(f"  Title       : {song.get('title', '?')}")
        if song.get("title_romanized"):
            lines.append(f"  Romanized   : {song['title_romanized']}")
        lines.append(f"  Duration    : {_fmt_duration(song.get('duration_seconds'))}")
        if song_artists_rows:
            by_role: dict[str, list[str]] = {}
            for ar in song_artists_rows:
                by_role.setdefault(str(ar["role"]), []).append(ar["name"])
            for role, names in by_role.items():
                lines.append(f"  {role:<12}: {', '.join(names)}")
        lines.append("")

    tt = str(task["task_type"])

    if tt == TaskType.INGEST_FAILED:
        lines.append("[bold underline]Data[/bold underline]")
        lines.append(f"  is_album hint : {data.get('is_album', '?')}")
        if data.get("extracted_timestamps"):
            lines.append(f"  timestamps    : {len(data['extracted_timestamps'])} from description")
        lines.append("")

        llm_match = data.get("llm_match")
        llm_cls = data.get("llm_classification")
        if llm_match and llm_match.get("best_match"):
            best = llm_match["best_match"]
            conf = llm_match.get("confidence", "?")
            conf_color = {"HIGH": "green", "MEDIUM": "yellow", "LOW": "red"}.get(conf, "white")
            vtype = llm_match.get("video_type", "?")
            lines.append(
                f"[bold underline]LLM Match[/bold underline]  "
                f"[{conf_color}]{vtype} · {conf}[/{conf_color}]  enriched {_age(task.get('llm_enriched_at'))}"
            )
            lines.append(f"  TouhouDB    : #{best['touhoudb_id']} — {best['name']}")
            lines.append(f"  Artist      : {best.get('artist_string', '?')}")
            tdb_dur = best.get("duration_seconds")
            vid_dur = video.get("duration_seconds") if video else None
            if tdb_dur and vid_dur:
                diff_pct = (vid_dur - tdb_dur) / tdb_dur * 100
                sign = "+" if diff_pct >= 0 else ""
                lines.append(
                    f"  Duration    : TouhouDB {_fmt_duration(tdb_dur)}, "
                    f"video {_fmt_duration(vid_dur)} ({sign}{diff_pct:.1f}%)"
                )
            bd = llm_match.get("score_breakdown", {})
            if bd:
                parts = [f"{k}={v:.2f}" for k, v in bd.items()]
                lines.append(f"  Score       : {', '.join(parts)}")
            cls = llm_match.get("classification", {})
            for field, label in [
                ("album_title", "Album"),
                ("release_event", "Event"),
                ("original_song_names", "Originals"),
            ]:
                val = cls.get(field)
                if val:
                    display = ", ".join(val) if isinstance(val, list) else val
                    lines.append(f"  {label:<12}: {display}")
            lines.append(f"  → Run [cyan]lotad tasks resolve {task_id}[/cyan] to ingest")
        elif llm_cls or (llm_match and not llm_match.get("best_match")):
            cls_data = llm_cls or (llm_match.get("classification") if llm_match else {}) or {}
            lines.append(
                f"[bold underline]LLM Classification[/bold underline]  "
                f"[red]no TouhouDB match[/red]  enriched {_age(task.get('llm_enriched_at'))}"
            )
            for field, label in [
                ("video_type", "Type"),
                ("song_title", "Title"),
                ("circle_name", "Circle"),
            ]:
                val = cls_data.get(field)
                if val:
                    lines.append(f"  {label:<12}: {val}")
            for field, label in [("arranger_names", "Arrangers"), ("vocalist_names", "Vocalists")]:
                val = cls_data.get(field)
                if val:
                    lines.append(f"  {label:<12}: {', '.join(val)}")
            for field, label in [
                ("album_title", "Album"),
                ("release_event", "Event"),
                ("original_song_names", "Originals"),
                ("extraction_notes", "Notes"),
            ]:
                val = cls_data.get(field)
                if val:
                    display = ", ".join(val) if isinstance(val, list) else val
                    lines.append(
                        f"  {label:<12}: {display!r}"
                        if field == "extraction_notes"
                        else f"  {label:<12}: {display}"
                    )
            lines.append(f"  → Run [cyan]lotad tasks resolve {task_id}[/cyan] to insert stub")
        else:
            lines.append(
                f"[dim]Not enriched yet — run [cyan]lotad tasks enrich --id {task_id}[/cyan][/dim]"
            )

    elif tt == TaskType.SUSPICIOUS_METADATA:
        tdb_dur = data.get("touhoudb_duration")
        yt_dur = data.get("youtube_duration")
        lines.append("[bold underline]Duration Discrepancy[/bold underline]")
        lines.append(f"  TouhouDB  : {_fmt_duration(tdb_dur)} ({tdb_dur}s)")
        if tdb_dur and yt_dur:
            diff_pct = (yt_dur - tdb_dur) / tdb_dur * 100
            sign = "+" if diff_pct >= 0 else ""
            lines.append(
                f"  YouTube   : {_fmt_duration(yt_dur)} ({yt_dur}s)  ← {sign}{diff_pct:.1f}%"
            )
        else:
            lines.append(f"  YouTube   : {_fmt_duration(yt_dur)} ({yt_dur}s)")
        lines.append("")

    elif tt == TaskType.DEDUPLICATE_SONGS:
        lines.append("[bold underline]Duplicate Playlists[/bold underline]")
        lines.append(
            f"  Playlist A (id={data.get('existing_playlist_id')})  source: {data.get('existing_source_type', '?')}"
        )
        lines.append(
            f"  Playlist B (id={data.get('new_playlist_id')})  source: {data.get('new_source_type', '?')}"
        )
        lines.append("")

    elif tt == TaskType.DROPPED_VIDEO:
        lines.append("[bold underline]Data[/bold underline]")
        lines.append(
            f"  position  : {data.get('position', '?')} in playlist {data.get('playlist_db_id', '?')}"
        )
        lines.append(f"  note      : {data.get('note', '?')}")
        lines.append("")

    elif tt == TaskType.FILL_MISSING_INFO:
        ids = data.get("original_touhoudb_ids", [])
        lines.append("[bold underline]Missing Originals[/bold underline]")
        lines.append(f"  TouhouDB original IDs not yet seeded: {ids}")
        lines.append("  → Run [cyan]lotad originals scrape[/cyan] to auto-resolve")
        lines.append("")

    else:
        lines.append("[bold underline]Data[/bold underline]")
        lines.append(json.dumps(data, ensure_ascii=False, indent=2))
        lines.append("")

    if str(task["status"]) == TaskStatus.OPEN:
        lines.append(
            f"[dim]Commands: [cyan]lotad tasks resolve {task_id}[/cyan]  "
            f"[cyan]lotad tasks dismiss {task_id}[/cyan][/dim]"
        )

    console.print(Panel("\n".join(lines), border_style="blue", padding=(1, 2)))


# ---------------------------------------------------------------------------
# dismiss
# ---------------------------------------------------------------------------


@tasks.command("dismiss")
@click.argument("task_id", type=int)
@click.option("--note", default=None, help="Optional note explaining the dismissal.")
def tasks_dismiss(task_id: int, note: str | None) -> None:
    """Dismiss a task (mark as false positive or permanently skip)."""
    engine = get_engine()
    with engine.begin() as conn:
        task_row = manager.get_task(conn, task_id)
        if task_row is None:
            console.print(f"[red]Task #{task_id} not found.[/red]")
            raise click.Abort()
        manager.dismiss_task(conn, task_id, note=note)
    console.print(f"[green]Dismissed task #{task_id}.[/green]")


# ---------------------------------------------------------------------------
# bulk-dismiss
# ---------------------------------------------------------------------------


@tasks.command("bulk-dismiss")
@click.option("--type", "task_type_str", required=True, help="Task type to bulk-dismiss.")
@click.option("--yes", is_flag=True, default=False, help="Confirm the operation (required).")
def tasks_bulk_dismiss(task_type_str: str, yes: bool) -> None:
    """Bulk-dismiss all OPEN tasks of a given type."""
    try:
        task_type = TaskType(task_type_str.upper())
    except ValueError:
        console.print(f"[red]Unknown task type: {task_type_str!r}[/red]")
        raise click.Abort() from None

    engine = get_engine()
    with engine.connect() as conn:
        counts = manager.count_tasks_by_type(conn, status=TaskStatus.OPEN)
        count = counts.get(task_type, 0)

    if count == 0:
        console.print(f"No OPEN {task_type.value} tasks to dismiss.")
        return

    console.print(f"This will dismiss [bold]{count}[/bold] OPEN {task_type.value} tasks.")
    if not yes:
        console.print("[yellow]Use --yes to confirm.[/yellow]")
        raise click.Abort()

    with engine.begin() as conn:
        dismissed = manager.bulk_dismiss_by_type(conn, task_type)
    console.print(f"[green]Dismissed {dismissed} {task_type.value} tasks.[/green]")


# ---------------------------------------------------------------------------
# resolve
# ---------------------------------------------------------------------------


@tasks.command("resolve")
@click.argument("task_id", type=int)
def tasks_resolve(task_id: int) -> None:
    """Interactively resolve a task."""
    engine = get_engine()
    with engine.connect() as conn:
        ctx = manager.get_task_with_context(conn, task_id)

    if ctx is None:
        console.print(f"[red]Task #{task_id} not found.[/red]")
        raise click.Abort()

    task = ctx["task"]
    tt = str(task["task_type"])

    if str(task["status"]) == TaskStatus.RESOLVED:
        console.print(f"[green]Task #{task_id} is already resolved.[/green]")
        return
    if str(task["status"]) == TaskStatus.DISMISSED:
        console.print(f"[dim]Task #{task_id} has been dismissed.[/dim]")
        return

    if tt == TaskType.INGEST_FAILED:
        asyncio.run(_resolve_ingest_failed(task_id, ctx))
    elif tt == TaskType.SUSPICIOUS_METADATA:
        _resolve_suspicious_metadata(task_id, ctx)
    elif tt == TaskType.DEDUPLICATE_SONGS:
        _resolve_deduplicate_songs(task_id, ctx)
    elif tt == TaskType.MISSING_LYRICIST:
        _resolve_missing_lyricist(task_id, ctx)
    elif tt == TaskType.DROPPED_VIDEO:
        _resolve_dropped_video(task_id, ctx)
    elif tt == TaskType.FILL_MISSING_INFO:
        _resolve_fill_missing_info(task_id, ctx)
    else:
        _resolve_generic(task_id, ctx)


# ---------------------------------------------------------------------------
# Classification display + interactive editor
# ---------------------------------------------------------------------------

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


def _print_classification_summary(cls: dict) -> None:
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


def _prompt_classification_overrides(cls: dict) -> dict:
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
        prompt_label = f"  {label}"
        # Show current value in brackets so the user can see it without retyping
        display = f"[{current}]" if current else "[empty]"
        console.print(f"{prompt_label} {display}")
        new_val = click.prompt(f"  → {label}", default=current, show_default=False)
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


# ---------------------------------------------------------------------------
# resolve wizards
# ---------------------------------------------------------------------------


async def _resolve_ingest_failed(task_id: int, ctx: dict) -> None:
    task = ctx["task"]
    video = ctx["video"]
    data = _get_data(task)

    console.print(f"\n[bold]Resolving INGEST_FAILED #{task_id}[/bold]")
    if video:
        console.print(
            f"Video: {video.get('title', '?')!r}  ({_fmt_duration(video.get('duration_seconds'))})"
        )
    console.print()

    llm_match = data.get("llm_match")
    llm_cls = data.get("llm_classification") or (
        llm_match.get("classification") if llm_match else None
    )
    has_match = llm_match and llm_match.get("best_match")

    if has_match:
        best = llm_match["best_match"]
        conf = llm_match.get("confidence", "?")
        conf_color = {"HIGH": "green", "MEDIUM": "yellow", "LOW": "red"}.get(conf, "white")
        console.print(
            f"LLM Match [[{conf_color}]{conf}[/{conf_color}]]: "
            f"#{best['touhoudb_id']} {best['name']!r} — {best.get('artist_string', '?')}  "
            f"({_fmt_duration(best.get('duration_seconds'))})"
        )
        console.print()
        console.print("[1] Accept LLM match → ingest via pipeline")
        console.print("[2] Enter a different TouhouDB song ID")
        console.print("[3] Composite video — enter comma-separated TouhouDB song IDs")
        console.print("[D] Dismiss")
        console.print("[Q] Quit")
        choice = click.prompt("Choice", default="1").strip().upper()

        if choice == "1":
            vtype = llm_match.get("video_type")
            if vtype == VideoType.FULL_ALBUM:
                track_ids = llm_match.get("album_track_touhoudb_ids") or []
                if track_ids:
                    top_tracks = (llm_match.get("classification") or {}).get("tracks") or []
                    hint_ts = (
                        [t.get("timestamp_seconds") for t in top_tracks[: len(track_ids)]]
                        if top_tracks
                        else None
                    )
                    await _do_ingest_composite(
                        task_id,
                        data,
                        video,
                        track_ids,
                        video_type=VideoType.FULL_ALBUM,
                        hint_timestamps=hint_ts,
                    )
                else:
                    console.print(
                        "[red]No track IDs in LLM match data. "
                        "Enter song IDs manually via option [3].[/red]"
                    )
            elif vtype == VideoType.COMPOSITE_TRACKS:
                track_results = llm_match.get("track_results") or []
                top_tracks = (llm_match.get("classification") or {}).get("tracks") or []
                # Zip before filtering so track_ids[i] and hint_ts[i] stay aligned.
                # Filtering track_results alone would shift hint indices for any
                # entry that lacks a best_match.
                pairs = [
                    (r, t)
                    for r, t in zip(track_results, top_tracks, strict=False)
                    if r.get("best_match")
                ]
                track_ids = [r["best_match"]["touhoudb_id"] for r, t in pairs]
                hint_ts = [t.get("timestamp_seconds") for r, t in pairs] if pairs else None
                if track_ids:
                    await _do_ingest_composite(
                        task_id,
                        data,
                        video,
                        track_ids,
                        video_type=VideoType.COMPOSITE_TRACKS,
                        hint_timestamps=hint_ts,
                    )
                else:
                    console.print(
                        "[red]No matched tracks in LLM data. "
                        "Enter song IDs manually via option [3].[/red]"
                    )
            else:
                await _do_ingest_single(task_id, data, video, best["touhoudb_id"])
        elif choice == "2":
            tdb_id = click.prompt("TouhouDB song ID", type=int)
            await _do_ingest_single(task_id, data, video, tdb_id)
        elif choice == "3":
            raw = click.prompt("Comma-separated TouhouDB song IDs")
            ids = [int(x.strip()) for x in raw.split(",") if x.strip().isdigit()]
            await _do_ingest_composite(
                task_id, data, video, ids, video_type=VideoType.COMPOSITE_TRACKS
            )
        elif choice == "D":
            with get_engine().begin() as conn:
                manager.dismiss_task(conn, task_id)
            console.print(f"[dim]Dismissed task #{task_id}.[/dim]")
        else:
            console.print("[dim]Quit.[/dim]")

    elif llm_cls:
        console.print("[yellow]LLM found no TouhouDB match.[/yellow]")
        _print_classification_summary(llm_cls)
        console.print()
        console.print("[1] Edit fields → insert song stub (no TouhouDB linkage)")
        console.print("[2] Accept as-is → insert song stub")
        console.print("[3] Enter a TouhouDB song ID to ingest from")
        console.print("[D] Dismiss")
        console.print("[Q] Quit")
        choice = click.prompt("Choice", default="D").strip().upper()

        if choice in ("1", "2"):
            if choice == "1":
                llm_cls = _prompt_classification_overrides(llm_cls)
            await _do_ingest_stub(task_id, data, video, llm_cls)
        elif choice == "3":
            tdb_id = click.prompt("TouhouDB song ID", type=int)
            await _do_ingest_single(task_id, data, video, tdb_id)
        elif choice == "D":
            with get_engine().begin() as conn:
                manager.dismiss_task(conn, task_id)
            console.print(f"[dim]Dismissed task #{task_id}.[/dim]")
        else:
            console.print("[dim]Quit.[/dim]")

    else:
        fail_count = data.get("enrich_fail_count") or 0
        if fail_count >= manager.ENRICH_FAIL_LIMIT:
            console.print(
                f"[yellow]LLM enrichment skipped after {fail_count} timeouts.[/yellow]"
            )
        else:
            console.print("[yellow]This task has not been enriched by the LLM yet.[/yellow]")
            console.print(
                f"  You can run [cyan]lotad tasks enrich --id {task_id}[/cyan] first, "
                f"or resolve manually below."
            )
        console.print()
        console.print("[1] Enter a TouhouDB song ID to ingest directly")
        console.print("[3] Composite video — enter comma-separated TouhouDB song IDs")
        console.print("[S] Insert song stub (song not on TouhouDB)")
        console.print("[D] Dismiss (not Touhou / permanently skip)")
        console.print("[Q] Quit")
        choice = click.prompt("Choice", default="Q").strip().upper()

        if choice == "1":
            tdb_id = click.prompt("TouhouDB song ID", type=int)
            await _do_ingest_single(task_id, data, video, tdb_id)
        elif choice == "3":
            raw = click.prompt("Comma-separated TouhouDB song IDs")
            ids = [int(x.strip()) for x in raw.split(",") if x.strip().isdigit()]
            await _do_ingest_composite(
                task_id, data, video, ids, video_type=VideoType.COMPOSITE_TRACKS
            )
        elif choice == "S":
            from lotad.agents.llm_extractor import VideoClassification

            seed = VideoClassification(
                video_type=VideoType.SINGLE_SONG,
                song_title=data.get("title", ""),
            ).model_dump()
            edited = _prompt_classification_overrides(seed)
            await _do_ingest_stub(task_id, data, video, edited)
        elif choice == "D":
            with get_engine().begin() as conn:
                manager.dismiss_task(conn, task_id)
            console.print(f"[dim]Dismissed task #{task_id}.[/dim]")


async def _do_ingest_single(task_id: int, data: dict, video_row: Any, touhoudb_id: int) -> None:
    import sqlalchemy as sa

    from lotad.config import get_settings
    from lotad.db.models import playlist_songs as ps_table
    from lotad.db.models import youtube_videos as yt_table
    from lotad.ingestion.pipeline import IngestPipeline
    from lotad.ingestion.youtube_client import PlaylistItem

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
    data: dict,
    video_row: Any,
    touhoudb_ids: list[int],
    *,
    video_type: VideoType,
    hint_timestamps: list[int | None] | None = None,
) -> None:
    import sqlalchemy as sa

    from lotad.config import get_settings
    from lotad.db.models import songs as songs_table
    from lotad.ingestion.pipeline import IngestPipeline
    from lotad.ingestion.youtube_client import PlaylistItem

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


async def _do_ingest_stub(task_id: int, data: dict, video_row: Any, llm_cls: dict) -> None:
    import sqlalchemy as sa

    from lotad.agents.llm_extractor import VideoClassification
    from lotad.db.models import youtube_videos as yt_table
    from lotad.ingestion.mappers import ingest_song_from_llm_classification

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


def _resolve_suspicious_metadata(task_id: int, ctx: dict) -> None:
    task = ctx["task"]
    song = ctx["song"]
    data = _get_data(task)

    tdb_dur = data.get("touhoudb_duration")
    yt_dur = data.get("youtube_duration")
    song_title = song["title"] if song else "?"

    console.print(f"\n[bold]Resolving SUSPICIOUS_METADATA #{task_id}[/bold]")
    console.print(f"Song: {song_title!r}")
    console.print(f"  TouhouDB  : {_fmt_duration(tdb_dur)} ({tdb_dur}s)")
    if tdb_dur and yt_dur:
        diff_pct = (yt_dur - tdb_dur) / tdb_dur * 100
        sign = "+" if diff_pct >= 0 else ""
        console.print(f"  YouTube   : {_fmt_duration(yt_dur)} ({yt_dur}s)  ← {sign}{diff_pct:.1f}%")
    else:
        console.print(f"  YouTube   : {_fmt_duration(yt_dur)} ({yt_dur}s)")
    console.print()
    console.print("[1] Accept TouhouDB duration (no DB change)")
    console.print(f"[2] Update song duration to YouTube value ({_fmt_duration(yt_dur)})")
    console.print("[3] Enter duration manually (seconds)")
    console.print("[D] Dismiss")
    console.print("[Q] Quit")
    choice = click.prompt("Choice", default="1").strip().upper()

    if choice == "D":
        with get_engine().begin() as conn:
            manager.dismiss_task(conn, task_id)
        console.print(f"[dim]Dismissed task #{task_id}.[/dim]")
        return
    if choice == "Q":
        console.print("[dim]Quit.[/dim]")
        return

    action_map = {"1": "accept_touhoudb", "2": "accept_youtube", "3": "manual"}
    action = action_map.get(choice)
    if not action:
        console.print("[dim]Quit.[/dim]")
        return

    corrected = None
    if choice == "3":
        corrected = click.prompt("Duration in seconds", type=int)

    if click.confirm("Proceed?", default=True):
        with get_engine().begin() as conn:
            manager.resolve_suspicious_metadata(
                conn, task_id, action=action, corrected_duration=corrected
            )
        console.print(f"[green]Resolved task #{task_id}.[/green]")


def _resolve_deduplicate_songs(task_id: int, ctx: dict) -> None:
    task = ctx["task"]
    song = ctx["song"]
    data = _get_data(task)

    song_title = song["title"] if song else "?"
    ex_pl = data.get("existing_playlist_id")
    ex_src = data.get("existing_source_type", "?")
    new_pl = data.get("new_playlist_id")
    new_src = data.get("new_source_type", "?")
    song_id = data.get("song_id") or task.get("related_song_id")

    console.print(f"\n[bold]Resolving DEDUPLICATE_SONGS #{task_id}[/bold]")
    console.print(f"Song: {song_title!r}")
    console.print(f"  Playlist A (id={ex_pl})  source: {ex_src}")
    console.print(f"  Playlist B (id={new_pl})  source: {new_src}")
    console.print()

    if ex_src == "INDIVIDUAL_VIDEO" and new_src == "COMPOSITE_VIDEO":
        console.print(
            "[1] Keep Playlist A / Individual  (recommended — individual takes precedence)"
        )
        console.print("[2] Keep Playlist B / Composite")
    elif ex_src == "COMPOSITE_VIDEO" and new_src == "INDIVIDUAL_VIDEO":
        console.print("[1] Keep Playlist A / Composite")
        console.print(
            "[2] Keep Playlist B / Individual  (recommended — individual takes precedence)"
        )
    else:
        console.print(f"[1] Keep Playlist A (id={ex_pl})")
        console.print(f"[2] Keep Playlist B (id={new_pl})")

    console.print("[3] Keep both playlists (intentional duplicate — dismiss)")
    console.print("[Q] Quit")
    choice = click.prompt("Choice", default="3").strip().upper()

    if choice == "1":
        remove_id = new_pl
    elif choice == "2":
        remove_id = ex_pl
    elif choice == "3":
        with get_engine().begin() as conn:
            manager.resolve_deduplicate_songs(conn, task_id, keep_both=True)
        console.print(f"[green]Keeping both. Task #{task_id} dismissed.[/green]")
        return
    else:
        console.print("[dim]Quit.[/dim]")
        return

    if click.confirm("Proceed?", default=True):
        with get_engine().begin() as conn:
            manager.resolve_deduplicate_songs(
                conn, task_id, remove_playlist_id=remove_id, song_id=song_id
            )
        console.print(
            f"[green]Removed from playlist {remove_id}. Task #{task_id} resolved.[/green]"
        )


def _resolve_missing_lyricist(task_id: int, ctx: dict) -> None:
    task = ctx["task"]
    song = ctx["song"]
    data = _get_data(task)

    song_title = song["title"] if song else "?"
    song_id = data.get("song_id") or task.get("related_song_id")

    console.print(f"\n[bold]Resolving MISSING_LYRICIST #{task_id}[/bold]")
    console.print(f"Song: {song_title!r}  (id={song_id})")
    console.print()
    console.print("[1] Add lyricist by name")
    console.print("[2] Mark as unknown / intentionally uncredited")
    console.print("[D] Dismiss (skip for now)")
    console.print("[Q] Quit")
    choice = click.prompt("Choice", default="2").strip().upper()

    if choice == "1":
        name = click.prompt("Lyricist name")
        if click.confirm(f"Add lyricist {name!r} to song?", default=True):
            with get_engine().begin() as conn:
                manager.resolve_missing_lyricist(conn, task_id, lyricist_name=name, song_id=song_id)
            console.print(f"[green]Added lyricist {name!r}. Task #{task_id} resolved.[/green]")
    elif choice == "2":
        if click.confirm("Mark lyricist as intentionally unknown?", default=True):
            with get_engine().begin() as conn:
                manager.resolve_missing_lyricist(conn, task_id, lyricist_name=None, song_id=song_id)
            console.print(f"[green]Task #{task_id} resolved (no lyricist).[/green]")
    elif choice == "D":
        with get_engine().begin() as conn:
            manager.dismiss_task(conn, task_id)
        console.print(f"[dim]Dismissed task #{task_id}.[/dim]")
    else:
        console.print("[dim]Quit.[/dim]")


def _resolve_dropped_video(task_id: int, ctx: dict) -> None:
    import sqlalchemy as sa

    from lotad.db.models import playlist_songs as ps_table

    task = ctx["task"]
    video = ctx["video"]
    data = _get_data(task)

    console.print(f"\n[bold]Resolving DROPPED_VIDEO #{task_id}[/bold]")
    if video:
        console.print(f"Video: {video.get('video_id')} — {video.get('title', '?')!r}")
    console.print(
        f"  Position {data.get('position', '?')} in playlist {data.get('playlist_db_id', '?')}"
    )
    console.print()

    # Check for a previously linked song
    linked_song_id = None
    if task["related_video_id"] is not None:
        with get_engine().connect() as conn:
            linked_song_id = conn.execute(
                sa.select(ps_table.c.song_id)
                .where(ps_table.c.youtube_video_id == task["related_video_id"])
                .limit(1)
            ).scalar_one_or_none()

    if linked_song_id:
        console.print(f"Previously linked song: songs.id={linked_song_id}")
        console.print()
        console.print(f"[1] Song confirmed — resolve with linked song_id={linked_song_id}")
        console.print("[2] Different song — enter songs.id manually")
        console.print("[D] Dismiss (video unidentifiable / replacement not needed)")
        console.print("[Q] Quit")
        default_choice = "1"
    else:
        console.print("[2] Song identified — enter songs.id manually")
        console.print("[D] Dismiss (video unidentifiable / replacement not needed)")
        console.print("[Q] Quit")
        default_choice = "D"

    choice = click.prompt("Choice", default=default_choice).strip().upper()

    if choice == "1" and linked_song_id:
        with get_engine().begin() as conn:
            manager.resolve_ingest_failed(conn, task_id, song_id=linked_song_id)
        console.print(f"[green]Task #{task_id} resolved.[/green]")
    elif choice == "2":
        sid = click.prompt("songs.id", type=int)
        if click.confirm("Proceed?", default=True):
            with get_engine().begin() as conn:
                manager.resolve_ingest_failed(conn, task_id, song_id=sid)
            console.print(f"[green]Task #{task_id} resolved.[/green]")
    elif choice == "D":
        with get_engine().begin() as conn:
            manager.dismiss_task(conn, task_id)
        console.print(f"[dim]Dismissed task #{task_id}.[/dim]")
    else:
        console.print("[dim]Quit.[/dim]")


def _resolve_fill_missing_info(task_id: int, ctx: dict) -> None:
    task = ctx["task"]
    data = _get_data(task)
    ids = data.get("original_touhoudb_ids", [])

    console.print(f"\n[bold]Resolving FILL_MISSING_INFO #{task_id}[/bold]")
    console.print(f"Missing original TouhouDB IDs: {ids}")
    console.print()
    console.print("Run [cyan]lotad originals scrape[/cyan] to auto-resolve, or:")
    console.print("[R] Retry linking now (if originals were recently scraped)")
    console.print("[D] Dismiss")
    console.print("[Q] Quit")
    choice = click.prompt("Choice", default="Q").strip().upper()

    if choice == "R":
        from lotad.cli.originals import _resolve_original_song_chain_tasks

        _resolve_original_song_chain_tasks()
        console.print("[green]Retried. Check status with `lotad tasks show`.[/green]")
    elif choice == "D":
        with get_engine().begin() as conn:
            manager.dismiss_task(conn, task_id)
        console.print(f"[dim]Dismissed task #{task_id}.[/dim]")
    else:
        console.print("[dim]Quit.[/dim]")


def _resolve_generic(task_id: int, ctx: dict) -> None:
    task = ctx["task"]
    data = _get_data(task)

    console.print(f"\n[bold]Task #{task_id}[/bold] ({task['task_type']})")
    console.print(json.dumps(data, ensure_ascii=False, indent=2))
    console.print()
    console.print("[D] Dismiss")
    console.print("[Q] Quit")
    choice = click.prompt("Choice", default="Q").strip().upper()
    if choice == "D":
        with get_engine().begin() as conn:
            manager.dismiss_task(conn, task_id)
        console.print(f"[dim]Dismissed task #{task_id}.[/dim]")


# ---------------------------------------------------------------------------
# enrich
# ---------------------------------------------------------------------------


@tasks.command("enrich")
@click.option("--id", "task_id", default=None, type=int, help="Enrich a specific task ID.")
@click.option(
    "--all",
    "enrich_all",
    is_flag=True,
    default=False,
    help="Enrich all unenriched INGEST_FAILED tasks (respects --limit).",
)
@click.option("--dry-run", is_flag=True, default=False, help="Print prompt without API calls.")
@click.option(
    "--limit",
    default=None,
    type=int,
    show_default=True,
    help="Max tasks to process. Implies --all when given without --id.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show full exception class name and stack trace on errors.",
)
def tasks_enrich(
    task_id: int | None, enrich_all: bool, dry_run: bool, limit: int | None, verbose: bool
) -> None:
    """Run LLM matching on INGEST_FAILED tasks to find TouhouDB matches."""
    # --limit alone implies --all (it already caps the scope)
    if limit is not None and not task_id:
        enrich_all = True
    if not task_id and not enrich_all:
        console.print("[yellow]Specify --id ID, --all, or --limit N.[/yellow]")
        raise click.Abort() from None
    asyncio.run(
        _run_enrich(
            task_id=task_id,
            enrich_all=enrich_all,
            dry_run=dry_run,
            limit=limit or 50,
            verbose=verbose,
        )
    )


def _identify_service(exc: Exception) -> str:
    """Return a short ALL-CAPS service label for an exception caught in the enrich loop.

    Checked in order:
    1. anthropic.APIError subclasses  → CLAUDE API
    2. URL on the request object      → TOUHOUDB or YOUTUBE API
    3. Module name heuristic          → best guess
    """
    try:
        import anthropic as _anthropic

        if isinstance(exc, _anthropic.APIError):
            return "CLAUDE API"
    except ImportError:
        pass

    req = getattr(exc, "request", None)
    if req is not None:
        url = str(req.url).lower()
        if "touhoudb" in url or "vocadb" in url:
            return "TOUHOUDB"
        if "googleapis" in url or "youtube" in url:
            return "YOUTUBE API"
        if "anthropic" in url:
            return "CLAUDE API"

    mod = type(exc).__module__ or ""
    if "anthropic" in mod:
        return "CLAUDE API"
    if "httpx" in mod:
        return "TOUHOUDB"  # httpx is only used for TouhouDB in this codebase
    if "google" in mod or "youtube" in mod:
        return "YOUTUBE API"

    return type(exc).__module__.split(".")[0].upper() or "UNKNOWN"


async def _run_enrich(
    *,
    task_id: int | None,
    enrich_all: bool,
    dry_run: bool,
    limit: int,
    verbose: bool = False,
) -> None:
    import sqlalchemy as sa

    from lotad.agents.llm_extractor import LLMExtractor, VideoType
    from lotad.config import get_settings
    from lotad.db.models import youtube_videos as yt_table
    from lotad.ingestion.pipeline import IngestPipeline
    from lotad.ingestion.touhoudb_client import TouhouDBClient
    from lotad.ingestion.youtube_client import PlaylistItem, YouTubeClient

    settings = get_settings()
    engine = get_engine()

    with engine.connect() as conn:
        if task_id is not None:
            task_row = manager.get_task(conn, task_id)
            if task_row is None:
                console.print(f"[red]Task #{task_id} not found.[/red]")
                return
            if str(task_row["task_type"]) != TaskType.INGEST_FAILED:
                console.print(f"[red]Task #{task_id} is not an INGEST_FAILED task.[/red]")
                return
            task_rows = [task_row]
        else:
            task_rows = manager.list_unenriched_ingest_failed(conn, limit=limit)

    if not task_rows:
        console.print("[green]No unenriched INGEST_FAILED tasks found.[/green]")
        return

    total = len(task_rows)
    console.print(f"Enriching {total} INGEST_FAILED task(s)...\n")

    auto_ingested = 0
    awaiting_review = 0
    unmatched = 0

    yt_client = YouTubeClient(settings)

    async with TouhouDBClient.from_settings(settings) as tdb:
        extractor = LLMExtractor(settings=settings, tdb_client=tdb, youtube_client=yt_client)

        for i, task_row in enumerate(task_rows, 1):
            tid = task_row["id"]
            data = _get_data(task_row)
            title = data.get("title", "?")
            short_title = title[:45] + "..." if len(title) > 48 else title

            with engine.connect() as conn:
                video_row = None
                if task_row["related_video_id"] is not None:
                    video_row = (
                        conn.execute(
                            sa.select(yt_table).where(yt_table.c.id == task_row["related_video_id"])
                        )
                        .mappings()
                        .one_or_none()
                    )

            if video_row is None:
                console.print(
                    f"[{i}/{total}] #{tid}  {short_title!r}  — [red]skipped (no video row)[/red]"
                )
                continue

            if dry_run:
                console.print(
                    f"[{i}/{total}] #{tid}  {short_title!r}  — [dim]dry-run: would call LLM[/dim]"
                )
                console.print(
                    f"  Input: title={video_row.get('title', '?')!r}  "
                    f"duration={video_row.get('duration_seconds')}s  "
                    f"is_album_hint={data.get('is_album', False)}"
                )
                continue

            result = None
            try:
                with engine.connect() as match_conn:
                    result = await extractor.find_match(
                        title=video_row.get("title") or "",
                        description=video_row.get("description") or "",
                        duration_seconds=video_row.get("duration_seconds"),
                        channel_name=video_row.get("channel_name"),
                        is_album_hint=data.get("is_album", False),
                        conn=match_conn,
                        youtube_video_id=video_row.get("video_id"),
                    )
            except Exception as exc:
                service = _identify_service(exc)
                if verbose:
                    import traceback

                    exc_name = f"{type(exc).__module__}.{type(exc).__name__}"
                    req = getattr(exc, "request", None)
                    url_hint = f" [{req.url}]" if req is not None else ""
                    console.print(
                        f"[{i}/{total}] #{tid}  {short_title!r}  "
                        f"— [red][{service}] {exc_name}{url_hint}: {exc!r}[/red]"
                    )
                    console.print(f"[dim]{traceback.format_exc()}[/dim]")
                else:
                    console.print(
                        f"[{i}/{total}] #{tid}  {short_title!r}  "
                        f"— [red][{service}] {type(exc).__name__}[/red]"
                    )
                # Track consecutive failures so the batch queue can skip tasks
                # that consistently time out (see manager.ENRICH_FAIL_LIMIT).
                fail_count = (_get_data(task_row).get("enrich_fail_count") or 0) + 1
                with engine.begin() as fc_conn:
                    manager.merge_task_data(fc_conn, tid, {"enrich_fail_count": fail_count})
                if fail_count >= manager.ENRICH_FAIL_LIMIT:
                    console.print(
                        f"  [dim]#{tid} has failed {fail_count} times — "
                        f"excluded from future batches. "
                        f"Use `lotad tasks resolve {tid}` to resolve manually.[/dim]"
                    )
                continue

            # Small inter-task delay to avoid hammering TouhouDB
            await asyncio.sleep(0.5)

            conf = result.confidence
            conf_color = {"HIGH": "green", "MEDIUM": "yellow", "LOW": "red"}.get(conf, "white")

            extra: dict = {}
            if result.best_match:
                extra["llm_match"] = result.model_dump(mode="json")
            else:
                extra["llm_classification"] = result.classification.model_dump(mode="json")

            with engine.begin() as conn:
                manager.merge_task_data(conn, tid, extra)

            # Auto-ingest HIGH confidence single_song matches only
            # (albums and composites require manual review to avoid bulk mistakes)
            if conf == "HIGH" and result.best_match and result.video_type == VideoType.SINGLE_SONG:
                playlist_db_id = data.get("playlist_db_id")
                if playlist_db_id:
                    item = PlaylistItem(
                        video_id=video_row["video_id"],
                        title=video_row.get("title") or "",
                        description=video_row.get("description") or "",
                        channel_name=video_row.get("channel_name") or "",
                        duration_seconds=video_row.get("duration_seconds"),
                        position=0,
                        is_available=True,
                    )
                    tdb_id = result.best_match.touhoudb_id
                    ok = False
                    try:
                        async with IngestPipeline(settings) as pipeline:
                            ok = await pipeline.ingest_video(
                                item,
                                playlist_db_id=playlist_db_id,
                                bulk_match={item.video_id: tdb_id},
                            )
                    except Exception as exc:
                        console.print(f"  [red]Pipeline error: {exc}[/red]")

                    if ok:
                        with engine.begin() as conn:
                            from lotad.db.models import playlist_songs as ps_table

                            yt_db_id = conn.execute(
                                sa.select(yt_table.c.id).where(yt_table.c.video_id == item.video_id)
                            ).scalar_one_or_none()
                            db_song_id = None
                            if yt_db_id:
                                db_song_id = conn.execute(
                                    sa.select(ps_table.c.song_id)
                                    .where(
                                        sa.and_(
                                            ps_table.c.youtube_video_id == yt_db_id,
                                            ps_table.c.removed_at.is_(None),
                                        )
                                    )
                                    .limit(1)
                                ).scalar_one_or_none()
                            manager.resolve_ingest_failed(conn, tid, song_id=db_song_id or tdb_id)
                        console.print(
                            f"[{i}/{total}] #{tid}  {short_title!r}  "
                            f"[{conf_color}]{conf}[/{conf_color}]  "
                            f"→ [green]ingested ✓[/green]  (TouhouDB #{tdb_id})"
                        )
                        auto_ingested += 1
                        continue
                    # Fall through to awaiting_review if pipeline failed

            if result.best_match:
                console.print(
                    f"[{i}/{total}] #{tid}  {short_title!r}  "
                    f"[{conf_color}]{conf}[/{conf_color}]  → awaiting review"
                )
                awaiting_review += 1
            else:
                console.print(
                    f"[{i}/{total}] #{tid}  {short_title!r}  "
                    f"[{conf_color}]{conf}[/{conf_color}]  → no match"
                )
                unmatched += 1

    if not dry_run:
        console.print(
            f"\n[bold]Done.[/bold]  "
            f"[green]{auto_ingested} auto-ingested[/green]  "
            f"[yellow]{awaiting_review} awaiting review[/yellow]  "
            f"[red]{unmatched} unmatched[/red]"
        )
        if awaiting_review > 0:
            console.print("  Run [cyan]lotad tasks list --type INGEST_FAILED[/cyan] to review.")
