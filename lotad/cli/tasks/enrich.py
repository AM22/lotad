"""tasks enrich command — LLM matching for INGEST_FAILED tasks."""

from __future__ import annotations

import asyncio
import traceback
from typing import Any

import click
import sqlalchemy as sa

from lotad.agents.llm_extractor import LLMExtractor, VideoType
from lotad.cli.tasks._group import tasks
from lotad.cli.tasks._shared import _CONFIDENCE_COLOR, _get_data, console
from lotad.config import get_settings
from lotad.db.models import TaskType
from lotad.db.models import playlist_songs as ps_table
from lotad.db.models import youtube_videos as yt_table
from lotad.db.session import get_engine
from lotad.ingestion.pipeline import IngestPipeline
from lotad.ingestion.touhoudb_client import TouhouDBClient
from lotad.ingestion.youtube_client import PlaylistItem, YouTubeClient
from lotad.tasks import manager


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
                video_row: Any = None
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
            conf_color = _CONFIDENCE_COLOR.get(conf, "white")

            extra: dict[str, Any] = {}
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
                            yt_db_id = conn.execute(
                                sa.select(yt_table.c.id).where(
                                    yt_table.c.video_id == item.video_id
                                )
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
