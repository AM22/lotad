"""lotad ingest — playlist and video ingestion commands."""

from __future__ import annotations

import asyncio
import logging

import click
from rich.cells import cell_len
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from lotad.config import get_settings
from lotad.ingestion.pipeline import IngestPipeline

console = Console()
logger = logging.getLogger(__name__)


@click.group()
def ingest() -> None:
    """Ingest YouTube playlists into LOTAD."""


@ingest.command("playlist")
@click.argument("playlist_id")
@click.option("--resume", is_flag=True, default=False, help="Resume from last checkpoint.")
@click.option("--limit", default=None, type=int, help="Stop after N videos (for testing).")
def ingest_playlist(playlist_id: str, resume: bool, limit: int | None) -> None:
    """Ingest all videos in PLAYLIST_ID into LOTAD.

    PLAYLIST_ID is the YouTube playlist ID
    (e.g. PLuDYUKEqeoaxodcKdDwsnjUBt1ZMsOa8q).
    """
    asyncio.run(_run_playlist(playlist_id, resume=resume, limit=limit))


@ingest.command("video")
@click.argument("video_id")
def ingest_video(video_id: str) -> None:
    """Ingest a single YouTube VIDEO_ID into LOTAD (useful for testing)."""
    asyncio.run(_run_video(video_id))


# ---------------------------------------------------------------------------
# Async runners
# ---------------------------------------------------------------------------


async def _run_playlist(playlist_id: str, *, resume: bool, limit: int | None) -> None:
    settings = get_settings()

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        task_id = progress.add_task("Ingesting\u2026", total=None)

        def on_progress(done: int, total: int, title: str) -> None:
            # Truncate by display width (wide chars like CJK/emoji count as 2 columns)
            max_width = 60
            width = 0
            cutoff = len(title)
            for i, ch in enumerate(title):
                w = cell_len(ch)
                if width + w > max_width:
                    cutoff = i
                    break
                width += w
            progress.update(
                task_id,
                completed=done,
                total=total,
                description=f"[bold blue]{title[:cutoff]}",
            )

        async with IngestPipeline(settings) as pipeline:
            stats = await pipeline.ingest_playlist(
                playlist_id,
                resume=resume,
                limit=limit,
                progress_callback=on_progress,
            )

    console.print(
        f"\n[green]Done![/green]  "
        f"matched={stats['matched']}  "
        f"unmatched={stats['unmatched']}  "
        f"errors={stats['errors']}  "
        f"skipped={stats['skipped']}"
    )


async def _run_video(video_id: str) -> None:
    settings = get_settings()

    from lotad.ingestion.youtube_client import PlaylistItem, YouTubeClient

    console.print(f"Ingesting video [bold]{video_id}[/bold]\u2026")

    yt = YouTubeClient(settings)
    item = yt.get_video(video_id)
    if item is None:
        console.print(
            "[yellow]Warning: could not fetch video metadata from YouTube; using stub.[/yellow]"
        )
        item = PlaylistItem(video_id=video_id, title=video_id)

    async with IngestPipeline(settings) as pipeline:
        matched = await pipeline.ingest_video(item)

    if matched:
        console.print("[green]\u2713 TouhouDB match found and saved.[/green]")
    else:
        console.print("[yellow]\u2717 No TouhouDB match. Task created for manual review.[/yellow]")
