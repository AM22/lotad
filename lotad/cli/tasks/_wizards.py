"""Interactive resolve wizards for each task type."""

from __future__ import annotations

import json
from typing import Any

import click
import sqlalchemy as sa

from lotad.agents.llm_extractor import VideoClassification, VideoType
from lotad.cli.originals import _resolve_original_song_chain_tasks
from lotad.cli.tasks._actions import (
    _do_ingest_composite,
    _do_ingest_single,
    _do_ingest_stub,
    _print_classification_summary,
    _prompt_classification_overrides,
    _prompt_timestamp_mode,
    _prompt_video_type_override,
)
from lotad.cli.tasks._shared import _CONFIDENCE_COLOR, _fmt_duration, _get_data, console
from lotad.db.models import playlist_songs as ps_table
from lotad.db.session import get_engine
from lotad.tasks import manager


async def _resolve_ingest_failed(task_id: int, ctx: dict[str, Any]) -> None:
    task = ctx["task"]
    video = ctx["video"]
    data = _get_data(task)

    console.print(f"\n[bold]Resolving INGEST_FAILED #{task_id}[/bold]")
    if video:
        console.print(
            f"Video: {video.get('title', '?')!r}  ({_fmt_duration(video.get('duration_seconds'))})"
        )
    console.print()

    while True:
        llm_match = data.get("llm_match")
        llm_cls = data.get("llm_classification") or (
            llm_match.get("classification") if llm_match else None
        )
        has_match = llm_match and llm_match.get("best_match")

        if has_match:
            best = llm_match["best_match"]
            conf = llm_match.get("confidence", "?")
            conf_color = _CONFIDENCE_COLOR.get(conf, "white")
            console.print(
                f"LLM Match [[{conf_color}]{conf}[/{conf_color}]]: "
                f"#{best['touhoudb_id']} {best['name']!r} — {best.get('artist_string', '?')}  "
                f"({_fmt_duration(best.get('duration_seconds'))})"
            )
            console.print()
            console.print("[1] Accept LLM match → ingest via pipeline")
            console.print("[2] Enter a different TouhouDB song ID")
            console.print("[3] Composite video — enter comma-separated TouhouDB song IDs")
            console.print("[4] Change video type classification")
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
                        timestamps = _prompt_timestamp_mode(len(track_ids), hint_timestamps=hint_ts)
                        await _do_ingest_composite(
                            task_id,
                            data,
                            video,
                            track_ids,
                            video_type=VideoType.COMPOSITE_TRACKS,
                            hint_timestamps=timestamps,
                        )
                    else:
                        console.print(
                            "[red]No matched tracks in LLM data. "
                            "Enter song IDs manually via option [3].[/red]"
                        )
                else:
                    await _do_ingest_single(task_id, data, video, best["touhoudb_id"])
                return
            elif choice == "2":
                tdb_id = click.prompt("TouhouDB song ID", type=int)
                await _do_ingest_single(task_id, data, video, tdb_id)
                return
            elif choice == "3":
                raw = click.prompt("Comma-separated TouhouDB song IDs")
                ids = [int(x.strip()) for x in raw.split(",") if x.strip().isdigit()]
                timestamps = _prompt_timestamp_mode(len(ids))
                await _do_ingest_composite(
                    task_id,
                    data,
                    video,
                    ids,
                    video_type=VideoType.COMPOSITE_TRACKS,
                    hint_timestamps=timestamps,
                )
                return
            elif choice == "4":
                new_vtype = _prompt_video_type_override(llm_match.get("video_type", "?"))
                data["llm_match"]["video_type"] = new_vtype
                continue
            elif choice == "D":
                with get_engine().begin() as conn:
                    manager.dismiss_task(conn, task_id)
                console.print(f"[dim]Dismissed task #{task_id}.[/dim]")
                return
            else:
                console.print("[dim]Quit.[/dim]")
                return

        elif llm_cls:
            console.print("[yellow]LLM found no TouhouDB match.[/yellow]")
            _print_classification_summary(llm_cls)
            console.print()
            console.print("[1] Edit fields → insert song stub (no TouhouDB linkage)")
            console.print("[2] Accept as-is → insert song stub")

            # Label option [3] based on the specific composite sub-type.
            vtype_str = llm_cls.get("video_type", "")
            is_full_album = vtype_str == VideoType.FULL_ALBUM
            is_composite_tracks = vtype_str == VideoType.COMPOSITE_TRACKS
            if is_full_album:
                console.print("[3] Enter a TouhouDB album ID to ingest as full album")
            elif is_composite_tracks:
                console.print(
                    "[3] Enter TouhouDB song IDs (comma-separated) to ingest as composite"
                )
            else:
                console.print("[3] Enter a TouhouDB song ID to ingest from")

            console.print("[D] Dismiss")
            console.print("[Q] Quit")
            choice = click.prompt("Choice", default="D").strip().upper()

            if choice in ("1", "2"):
                if choice == "1":
                    llm_cls = _prompt_classification_overrides(llm_cls)
                await _do_ingest_stub(task_id, data, video, llm_cls)
            elif choice == "3":
                if is_full_album:
                    from lotad.config import get_settings
                    from lotad.ingestion.touhoudb_client import TouhouDBClient

                    album_id = click.prompt("TouhouDB album ID", type=int)
                    settings = get_settings()
                    async with TouhouDBClient.from_settings(settings) as tdb:
                        album = await tdb.get_album(album_id)
                    track_ids = [t.song.id for t in album.tracks if t.song is not None]
                    console.print(
                        f"[green]Album:[/green] #{album.id} {album.name!r} "
                        f"— {len(track_ids)} tracks"
                    )
                    for i, t in enumerate(album.tracks, 1):
                        name = t.song.name if t.song else "(no song)"
                        sid = f"#{t.song.id}" if t.song else "—"
                        console.print(f"  {i:>2}. {sid}  {name}")
                    if not track_ids:
                        console.print("[red]No linked song IDs found on this album.[/red]")
                        continue
                    timestamps = _prompt_timestamp_mode(len(track_ids))
                    await _do_ingest_composite(
                        task_id,
                        data,
                        video,
                        track_ids,
                        video_type=VideoType.FULL_ALBUM,
                        hint_timestamps=timestamps,
                    )
                elif is_composite_tracks:
                    raw = click.prompt("Comma-separated TouhouDB song IDs")
                    ids = [int(x.strip()) for x in raw.split(",") if x.strip().isdigit()]
                    timestamps = _prompt_timestamp_mode(len(ids))
                    await _do_ingest_composite(
                        task_id,
                        data,
                        video,
                        ids,
                        video_type=VideoType.COMPOSITE_TRACKS,
                        hint_timestamps=timestamps,
                    )
                else:
                    tdb_id = click.prompt("TouhouDB song ID", type=int)
                    await _do_ingest_single(task_id, data, video, tdb_id)
            elif choice == "D":
                with get_engine().begin() as conn:
                    manager.dismiss_task(conn, task_id)
                console.print(f"[dim]Dismissed task #{task_id}.[/dim]")
            else:
                console.print("[dim]Quit.[/dim]")
            return

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
                timestamps = _prompt_timestamp_mode(len(ids))
                await _do_ingest_composite(
                    task_id,
                    data,
                    video,
                    ids,
                    video_type=VideoType.COMPOSITE_TRACKS,
                    hint_timestamps=timestamps,
                )
            elif choice == "S":
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
            return


def _resolve_suspicious_metadata(task_id: int, ctx: dict[str, Any]) -> None:
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


def _resolve_deduplicate_songs(task_id: int, ctx: dict[str, Any]) -> None:
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


def _resolve_missing_lyricist(task_id: int, ctx: dict[str, Any]) -> None:
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


def _resolve_dropped_video(task_id: int, ctx: dict[str, Any]) -> None:
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


def _resolve_fill_missing_info(task_id: int, ctx: dict[str, Any]) -> None:
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
        _resolve_original_song_chain_tasks()
        console.print("[green]Retried. Check status with `lotad tasks show`.[/green]")
    elif choice == "D":
        with get_engine().begin() as conn:
            manager.dismiss_task(conn, task_id)
        console.print(f"[dim]Dismissed task #{task_id}.[/dim]")
    else:
        console.print("[dim]Quit.[/dim]")


def _resolve_generic(task_id: int, ctx: dict[str, Any]) -> None:
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
