"""tasks show command."""

from __future__ import annotations

import json
from typing import Any

import click
from rich.panel import Panel

from lotad.cli.tasks._group import tasks
from lotad.cli.tasks._shared import (
    _CONFIDENCE_COLOR,
    _STATUS_COLOR,
    _age,
    _fmt_duration,
    _get_data,
    console,
)
from lotad.db.models import TaskStatus, TaskType
from lotad.db.session import get_engine
from lotad.tasks import manager


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

    status_color = _STATUS_COLOR.get(str(task["status"]), "white")

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
        _render_ingest_failed_section(lines, data, video, task, task_id)
    elif tt == TaskType.SUSPICIOUS_METADATA:
        _render_suspicious_metadata_section(lines, data)
    elif tt == TaskType.DEDUPLICATE_SONGS:
        _render_deduplicate_songs_section(lines, data)
    elif tt == TaskType.DROPPED_VIDEO:
        _render_dropped_video_section(lines, data)
    elif tt == TaskType.FILL_MISSING_INFO:
        _render_fill_missing_info_section(lines, data)
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


def _render_ingest_failed_section(
    lines: list[str],
    data: dict[str, Any],
    video: Any,
    task: Any,
    task_id: int,
) -> None:
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
        conf_color = _CONFIDENCE_COLOR.get(conf, "white")
        vtype = llm_match.get("video_type", "?")
        enriched_age = _age(task.get("llm_enriched_at"))
        lines.append(
            f"[bold underline]LLM Match[/bold underline]  "
            f"[{conf_color}]{vtype} · {conf}[/{conf_color}]  enriched {enriched_age}"
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


def _render_suspicious_metadata_section(lines: list[str], data: dict[str, Any]) -> None:
    tdb_dur = data.get("touhoudb_duration")
    yt_dur = data.get("youtube_duration")
    lines.append("[bold underline]Duration Discrepancy[/bold underline]")
    lines.append(f"  TouhouDB  : {_fmt_duration(tdb_dur)} ({tdb_dur}s)")
    if tdb_dur and yt_dur:
        diff_pct = (yt_dur - tdb_dur) / tdb_dur * 100
        sign = "+" if diff_pct >= 0 else ""
        lines.append(f"  YouTube   : {_fmt_duration(yt_dur)} ({yt_dur}s)  ← {sign}{diff_pct:.1f}%")
    else:
        lines.append(f"  YouTube   : {_fmt_duration(yt_dur)} ({yt_dur}s)")
    lines.append("")


def _render_deduplicate_songs_section(lines: list[str], data: dict[str, Any]) -> None:
    lines.append("[bold underline]Duplicate Playlists[/bold underline]")
    ex_src = data.get("existing_source_type", "?")
    new_src = data.get("new_source_type", "?")
    lines.append(f"  Playlist A (id={data.get('existing_playlist_id')})  source: {ex_src}")
    lines.append(f"  Playlist B (id={data.get('new_playlist_id')})  source: {new_src}")
    lines.append("")


def _render_dropped_video_section(lines: list[str], data: dict[str, Any]) -> None:
    lines.append("[bold underline]Data[/bold underline]")
    lines.append(
        f"  position  : {data.get('position', '?')} in playlist {data.get('playlist_db_id', '?')}"
    )
    lines.append(f"  note      : {data.get('note', '?')}")
    lines.append("")


def _render_fill_missing_info_section(lines: list[str], data: dict[str, Any]) -> None:
    ids = data.get("original_touhoudb_ids", [])
    lines.append("[bold underline]Missing Originals[/bold underline]")
    lines.append(f"  TouhouDB original IDs not yet seeded: {ids}")
    lines.append("  → Run [cyan]lotad originals scrape[/cyan] to auto-resolve")
    lines.append("")
