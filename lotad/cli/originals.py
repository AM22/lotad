"""lotad originals — original song management commands."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

import click
import sqlalchemy as sa
from rich.console import Console

from lotad.config import get_settings
from lotad.db.models import TaskStatus, TaskType, tasks
from lotad.db.session import get_engine
from lotad.ingestion.mappers import (
    link_original_song_characters,
    link_song_originals,
    match_work_for_song,
    upsert_original_song,
)
from lotad.ingestion.touhoudb_client import TouhouDBClient
from lotad.ingestion.touhoudb_models import SongDetail

console = Console()
logger = logging.getLogger(__name__)

# TouhouDB artist IDs for original song composers
_ZUN_ARTIST_ID = 1
_U2_AKIYAMA_ARTIST_ID = 45


@click.group()
def originals() -> None:
    """Manage original ZUN songs in LOTAD."""


@originals.command("scrape")
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print what would be inserted without writing to the database.",
)
@click.option(
    "--limit",
    default=None,
    type=int,
    help="Process at most N songs (useful with --dry-run for testing).",
)
def scrape(dry_run: bool, limit: int | None) -> None:
    """Scrape all original songs by ZUN and U2 Akiyama from TouhouDB.

    Upserts them into original_songs, links characters via
    original_song_characters (confidence=MEDIUM), then resolves any open
    FILL_MISSING_INFO tasks whose original_touhoudb_ids match a newly
    inserted song.
    """
    asyncio.run(_run_scrape(dry_run=dry_run, limit=limit))


async def _run_scrape(*, dry_run: bool, limit: int | None) -> None:
    settings = get_settings()
    engine = get_engine()

    if dry_run:
        console.print("[yellow]DRY RUN — no changes will be written[/yellow]")

    async with TouhouDBClient.from_settings(settings) as client:
        # Fetch originals from both composers, deduplicate by touhoudb_id
        console.print(f"Fetching Original songs by ZUN (artist_id={_ZUN_ARTIST_ID})…")
        zun_songs = await client.get_songs_by_artist(_ZUN_ARTIST_ID)

        console.print(f"Fetching Original songs by U2 Akiyama (artist_id={_U2_AKIYAMA_ARTIST_ID})…")
        u2_songs = await client.get_songs_by_artist(_U2_AKIYAMA_ARTIST_ID)

    all_songs_by_id = {s.id: s for s in zun_songs}
    for s in u2_songs:
        all_songs_by_id.setdefault(s.id, s)
    all_songs = list(all_songs_by_id.values())
    if limit is not None:
        all_songs = all_songs[:limit]

    console.print(f"Total unique original songs to process: [bold]{len(all_songs)}[/bold]")

    stats = {
        "upserted": 0,
        "characters_linked": 0,
        "skipped_no_work": 0,
        "tasks_resolved": 0,
    }

    if dry_run:
        with engine.connect() as conn:
            for detail in all_songs:
                work_id = match_work_for_song(detail.albums, conn)
                if work_id is None:
                    console.print(
                        f"  [red]SKIP[/red] {detail.name!r} (touhoudb_id={detail.id})"
                        " — no work match"
                    )
                    stats["skipped_no_work"] += 1
                    continue
                stage = _stage_label(detail)
                char_count = sum(
                    1
                    for c in detail.artists
                    if c.artist and c.artist.artistType.lower() == "character"
                )
                console.print(
                    f"  [green]WOULD INSERT[/green] {detail.name!r}"
                    f" (touhoudb_id={detail.id}, work_id={work_id},"
                    f" stage={stage}, characters={char_count})"
                )
                stats["upserted"] += 1
                stats["characters_linked"] += char_count
    else:
        with engine.begin() as conn:
            for detail in all_songs:
                work_id = match_work_for_song(detail.albums, conn)
                if work_id is None:
                    console.print(
                        f"  [red]SKIP[/red] {detail.name!r} (touhoudb_id={detail.id})"
                        " — no work match"
                    )
                    stats["skipped_no_work"] += 1
                    continue

                original_song_id = upsert_original_song(detail, work_id, conn)
                stats["upserted"] += 1

                chars = link_original_song_characters(original_song_id, detail, conn)
                stats["characters_linked"] += chars

            # Resolve FILL_MISSING_INFO tasks now that original_songs has touhoudb_ids
            resolved = _resolve_original_song_chain_tasks(conn)
            stats["tasks_resolved"] = resolved

    _print_summary(stats, dry_run=dry_run)


def _resolve_original_song_chain_tasks(conn: sa.Connection) -> int:
    """
    Resolve FILL_MISSING_INFO tasks raised because an original song's chain was
    not yet in the DB.  Now that ``original_songs`` has been populated with
    ``touhoudb_id`` values, attempt to link ``song_originals`` for each open task
    and mark it RESOLVED when all originals are successfully linked.

    Returns the number of tasks resolved.
    """
    open_tasks = conn.execute(
        sa.select(tasks).where(
            tasks.c.task_type == TaskType.FILL_MISSING_INFO,
            tasks.c.status == TaskStatus.OPEN,
        )
    ).fetchall()

    resolved_count = 0
    for task in open_tasks:
        data = task.data or {}
        song_id: int | None = data.get("song_id")
        original_touhoudb_ids: list[int] = data.get("original_touhoudb_ids", [])

        if song_id is None or not original_touhoudb_ids:
            continue

        linked = link_song_originals(song_id, original_touhoudb_ids, conn)
        if len(linked) == len(original_touhoudb_ids):
            conn.execute(
                tasks.update()
                .where(tasks.c.id == task.id)
                .values(
                    status=TaskStatus.RESOLVED,
                    resolved_at=datetime.now(UTC),
                )
            )
            resolved_count += 1
            logger.debug("Resolved FILL_MISSING_INFO task id=%d (song_id=%d)", task.id, song_id)
        elif linked:
            # Partial — some originals linked but not all; leave open
            logger.warning(
                "FILL_MISSING_INFO task id=%d (song_id=%d): linked %d/%d originals — leaving OPEN",
                task.id,
                song_id,
                len(linked),
                len(original_touhoudb_ids),
            )

    return resolved_count


def _stage_label(detail: SongDetail) -> str:
    """Return a human-readable stage label for dry-run output."""
    from lotad.ingestion.mappers import _parse_stage_from_tags

    stage = _parse_stage_from_tags(detail.tags)
    if stage is None:
        return "unknown"
    labels = {0: "title", 7: "extra", 8: "ending", 9: "staff roll"}
    return labels.get(stage, f"stage {stage}")


def _print_summary(stats: dict[str, int], *, dry_run: bool) -> None:
    prefix = "[yellow]DRY RUN[/yellow] — " if dry_run else ""
    console.print(
        f"\n{prefix}[bold]Done.[/bold]\n"
        f"  Original songs upserted : {stats['upserted']}\n"
        f"  Characters linked        : {stats['characters_linked']}\n"
        f"  Songs skipped (no work)  : {stats['skipped_no_work']}\n"
        f"  Tasks resolved           : {stats['tasks_resolved']}"
    )
