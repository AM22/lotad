"""lotad sync — continuous playlist sync and manual metadata refresh."""

from __future__ import annotations

import asyncio
import logging

import click
import sqlalchemy as sa
from rich.console import Console
from rich.table import Table

from lotad.config import get_settings
from lotad.db.models import playlists
from lotad.db.session import get_engine
from lotad.sync.metadata_refresh import (
    FILTER_NAMES,
    read_song_ids_from_csv,
    refresh_songs,
    select_all_refreshable_songs,
    select_songs_for_filter,
)
from lotad.sync.playlist_sync import SyncReport, sync_playlists

console = Console()
logger = logging.getLogger(__name__)


@click.group()
def sync() -> None:
    """Sync playlists and refresh metadata."""


# ---------------------------------------------------------------------------
# lotad sync playlist <name|youtube_id>
# lotad sync all
# ---------------------------------------------------------------------------


@sync.command("playlist")
@click.argument("playlist")
@click.option(
    "--retry-stubs/--no-retry-stubs",
    default=True,
    help="Opportunistically re-match stub songs against TouhouDB after sync. Default: on.",
)
@click.option(
    "--limit", default=None, type=int, help="Stop after N videos per playlist (for testing)."
)
def sync_playlist(playlist: str, retry_stubs: bool, limit: int | None) -> None:
    """Sync a single playlist by name or YouTube playlist ID."""
    youtube_playlist_id = _resolve_playlist_arg(playlist)
    report = asyncio.run(
        sync_playlists(
            [youtube_playlist_id],
            retry_stubs=retry_stubs,
            limit=limit,
        )
    )
    _print_sync_report(report)


@sync.command("all")
@click.option(
    "--retry-stubs/--no-retry-stubs",
    default=True,
    help="Opportunistically re-match stub songs against TouhouDB after sync. Default: on.",
)
@click.option(
    "--limit", default=None, type=int, help="Stop after N videos per playlist (for testing)."
)
def sync_all(retry_stubs: bool, limit: int | None) -> None:
    """Sync every tracked playlist."""
    report = asyncio.run(sync_playlists(None, retry_stubs=retry_stubs, limit=limit))
    _print_sync_report(report)


def _resolve_playlist_arg(arg: str) -> str:
    """Accept either a YouTube playlist ID or our internal playlist name."""
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            sa.select(playlists.c.youtube_playlist_id).where(
                sa.or_(
                    playlists.c.youtube_playlist_id == arg,
                    playlists.c.name == arg,
                )
            )
        ).first()
    if row is None:
        raise click.BadParameter(
            f"No playlist found with name or YouTube ID {arg!r}. "
            "Use `lotad db seed-playlists` first if needed."
        )
    return row[0]


def _print_sync_report(report: SyncReport) -> None:
    table = Table(title="Sync results")
    table.add_column("Playlist")
    table.add_column("Kept", justify="right")
    table.add_column("Added", justify="right")
    table.add_column("Moved in", justify="right")
    table.add_column("Moved out", justify="right")
    table.add_column("Same-song swap", justify="right")
    table.add_column("Silent → unsaved", justify="right")
    table.add_column("Drop task", justify="right")
    table.add_column("Dead replaced", justify="right")
    table.add_column("Newly unavailable", justify="right")
    table.add_column("Errors", justify="right")
    for name, outcome in report.per_playlist.items():
        table.add_row(
            name,
            str(outcome.kept),
            str(outcome.added),
            str(outcome.moved_in),
            str(outcome.moved_out),
            str(outcome.same_song_swap),
            str(outcome.silent_drop),
            str(outcome.task_drop),
            str(outcome.dead_replacement),
            str(outcome.deleted_in_place),
            str(outcome.errors),
        )
    console.print(table)
    console.print(
        f"Stub retry: [green]{report.stub_promoted}[/green] promoted, "
        f"[dim]{report.stub_no_match}[/dim] still no match"
    )
    console.print(f"Dedup tasks reconciled: {report.dedup_tasks_reconciled}")
    if report.errors:
        console.print(f"[red]Run errors: {report.errors}[/red]")


# ---------------------------------------------------------------------------
# lotad sync refresh-metadata
# ---------------------------------------------------------------------------


@sync.command("refresh-metadata")
@click.option("--csv", "csv_path", type=click.Path(exists=True, dir_okay=False), default=None)
@click.option(
    "--filter",
    "filter_name",
    type=click.Choice(list(FILTER_NAMES)),
    default=None,
    help="Preset selector for songs to refresh.",
)
@click.option("--song-id", "song_ids", type=int, multiple=True, help="One-off refresh; repeatable.")
@click.option(
    "--all",
    "refresh_all",
    is_flag=True,
    default=False,
    help="Refresh every song with a TouhouDB ID.",
)
@click.option("--dry-run", is_flag=True, default=False, help="Report selection without writing.")
def refresh_metadata(
    csv_path: str | None,
    filter_name: str | None,
    song_ids: tuple[int, ...],
    refresh_all: bool,
    dry_run: bool,
) -> None:
    """Re-pull TouhouDB metadata for an explicit set of songs."""
    sources = sum(bool(x) for x in (csv_path, filter_name, song_ids, refresh_all))
    if sources != 1:
        raise click.BadParameter("Specify exactly one of --csv, --filter, --song-id, or --all.")

    settings = get_settings()
    engine = get_engine()

    with engine.connect() as conn:
        if csv_path:
            ids = read_song_ids_from_csv(csv_path)
        elif filter_name:
            ids = select_songs_for_filter(filter_name, conn)
        elif song_ids:
            ids = list(song_ids)
        else:
            ids = select_all_refreshable_songs(conn)
            console.print(
                f"[yellow]About to refresh {len(ids)} songs from TouhouDB.[/yellow] "
                "This will hit the API once per song."
            )
            click.confirm("Continue?", abort=True)

    console.print(f"Selected [bold]{len(ids)}[/bold] song(s) for refresh.")

    if not ids:
        return

    report = asyncio.run(refresh_songs(ids, settings=settings, dry_run=dry_run))

    table = Table(title="Refresh results")
    table.add_column("Bucket")
    table.add_column("Count", justify="right")
    table.add_row("Refreshed", str(report.refreshed))
    table.add_row("Stub promoted", str(report.stub_promoted))
    table.add_row("Stub unchanged", str(report.stub_unchanged))
    table.add_row("Skipped (no upstream)", str(report.skipped_no_upstream))
    table.add_row("Errors", str(report.errors))
    console.print(table)
