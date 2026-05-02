"""lotad contrib add-videos — bulk-submit YouTube PV draft edits to TouhouDB."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import click
import httpx
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from lotad.cli.contrib._group import contrib
from lotad.config import get_settings
from lotad.contrib.candidate_finder import VideoLinkCandidate, find_candidates
from lotad.contrib.touhoudb_session import (
    AuthenticationError,
    SessionNotLoaded,
    TouhouDBSession,
)
from lotad.contrib.touhoudb_writer import PVAlreadyPresent, TouhouDBWriter
from lotad.db.session import get_engine

console = Console()
logger = logging.getLogger(__name__)

# Edit-changelog message recorded on every draft submission so a TouhouDB
# moderator (you) can tell at a glance which edits came from this tool.
_UPDATE_NOTES = "Added YouTube PV from LOTAD (auto-generated draft for review)."


@contrib.command("add-videos")
@click.option(
    "--limit",
    "-n",
    type=int,
    default=None,
    help="Stop after N candidates (recommended for first runs).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print what would be submitted without making any TouhouDB POSTs.",
)
def add_videos_cmd(limit: int | None, dry_run: bool) -> None:
    """Submit draft YouTube PV edits to TouhouDB for songs missing one.

    Uses the saved session from `lotad contrib login`. Each edit posts as
    Status=Draft so you can review on touhoudb.com before publishing.
    """
    asyncio.run(_run_add_videos(limit=limit, dry_run=dry_run))


async def _run_add_videos(*, limit: int | None, dry_run: bool) -> None:
    settings = get_settings()
    session_path = Path(settings.touhoudb_session_path)

    engine = get_engine()
    with engine.connect() as conn:
        candidates = find_candidates(conn, limit=limit)

    if not candidates:
        console.print("[yellow]No candidates found.[/yellow]")
        return

    console.print(f"Found [bold]{len(candidates)}[/bold] candidate(s).")
    _print_preview_table(candidates[: min(10, len(candidates))])
    if len(candidates) > 10:
        console.print(f"[dim]…and {len(candidates) - 10} more.[/dim]")

    if dry_run:
        console.print("\n[cyan]Dry run — no edits submitted.[/cyan]")
        return

    submitted: list[VideoLinkCandidate] = []
    skipped: list[tuple[VideoLinkCandidate, str]] = []
    failed: list[tuple[VideoLinkCandidate, str]] = []

    async with TouhouDBSession.from_settings(settings) as session:
        try:
            session.load(session_path)
            await session.verify()
        except SessionNotLoaded as exc:
            console.print(f"[red]No session:[/red] {exc}")
            raise click.exceptions.Exit(1) from None
        except AuthenticationError as exc:
            console.print(
                f"[red]Session expired:[/red] {exc}\n"
                f"Run [bold]lotad contrib login[/bold] to re-authenticate."
            )
            raise click.exceptions.Exit(1) from None

        writer = TouhouDBWriter(session)

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("Submitting drafts…", total=len(candidates))
            for cand in candidates:
                progress.update(
                    task_id,
                    description=f"S/{cand.touhoudb_id} ← {cand.video_id}",
                )
                try:
                    await writer.add_youtube_pv(
                        song_id=cand.touhoudb_id,
                        video_id=cand.video_id,
                        video_url=cand.video_url,
                        channel_name=cand.channel_name,
                        video_title=cand.video_title,
                        video_length_seconds=cand.video_duration_seconds,
                        pv_type=cand.suggested_pv_type,
                        update_notes=_UPDATE_NOTES,
                    )
                    submitted.append(cand)
                except PVAlreadyPresent as exc:
                    skipped.append((cand, f"PV already present: {exc.existing_video_ids!r}"))
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 401:
                        # Cookie died mid-run; nothing useful to do for the rest.
                        console.print(
                            "\n[red]Session expired during run.[/red] "
                            "Run [bold]lotad contrib login[/bold] and re-run."
                        )
                        failed.append((cand, "session expired"))
                        break
                    failed.append((cand, f"HTTP {exc.response.status_code}"))
                except Exception as exc:  # noqa: BLE001 — top-level CLI guard
                    logger.exception("Unexpected error submitting draft for %r", cand)
                    failed.append((cand, repr(exc)))
                finally:
                    progress.advance(task_id)

    _print_summary(submitted, skipped, failed)


def _print_preview_table(candidates: list[VideoLinkCandidate]) -> None:
    table = Table(title="Preview")
    table.add_column("LOTAD song", justify="right")
    table.add_column("TouhouDB S/")
    table.add_column("Video ID")
    table.add_column("PVType")
    table.add_column("Channel")
    table.add_column("Title", overflow="ellipsis", max_width=40)
    for c in candidates:
        table.add_row(
            str(c.song_id),
            str(c.touhoudb_id),
            c.video_id,
            c.suggested_pv_type,
            c.channel_name or "—",
            c.song_title,
        )
    console.print(table)


def _print_summary(
    submitted: list[VideoLinkCandidate],
    skipped: list[tuple[VideoLinkCandidate, str]],
    failed: list[tuple[VideoLinkCandidate, str]],
) -> None:
    console.print(
        f"\n[green]submitted={len(submitted)}[/green]  "
        f"[yellow]skipped={len(skipped)}[/yellow]  "
        f"[red]failed={len(failed)}[/red]"
    )
    if skipped:
        console.print("\n[yellow]Skipped:[/yellow]")
        for c, reason in skipped[:20]:
            console.print(f"  S/{c.touhoudb_id} ({c.video_id}): {reason}")
        if len(skipped) > 20:
            console.print(f"  …and {len(skipped) - 20} more.")
    if failed:
        console.print("\n[red]Failed:[/red]")
        for c, reason in failed[:20]:
            console.print(f"  S/{c.touhoudb_id} ({c.video_id}): {reason}")
        if len(failed) > 20:
            console.print(f"  …and {len(failed) - 20} more.")
