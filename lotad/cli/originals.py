"""lotad originals — original song management commands."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

import click
import sqlalchemy as sa
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeElapsedColumn

from lotad.config import get_settings
from lotad.db.models import AppearanceType, ConfidenceLevel, TaskStatus, TaskType, tasks
from lotad.db.session import get_engine
from lotad.ingestion.mappers import (
    link_original_song_characters,
    link_song_originals,
    match_work_for_song,
    upsert_original_song,
)
from lotad.ingestion.touhoudb_client import TouhouDBClient
from lotad.ingestion.touhoudb_models import SongDetail
from lotad.ingestion.touhouwiki_client import TouhouWikiClient
from lotad.ingestion.touhouwiki_parser import (
    extract_japanese_title,
    parse_section_heading,
)
from lotad.ingestion.wiki_enrichment import (
    create_fill_missing_info_task,
    create_review_character_mapping_task,
    get_work_by_short_name,
    link_character_to_original,
    list_originals_for_work,
    match_character_by_name,
    match_original_song,
    update_original_song_stage_boss,
    upsert_character_work,
)
from lotad.ingestion.wiki_game_slugs import GAMES, GAMES_BY_SHORT_NAME, GAMES_BY_SLUG, WikiGame

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
        "no_work": 0,
        "tasks_resolved": 0,
    }

    if dry_run:
        with engine.connect() as conn:
            for detail in all_songs:
                work_id = match_work_for_song(detail.albums, conn)
                stage = _stage_label(detail)
                char_count = sum(
                    1
                    for c in detail.artists
                    if c.artist and c.artist.artistType.lower() == "character"
                )
                work_str = f"work_id={work_id}" if work_id is not None else "no work"
                console.print(
                    f"  [green]WOULD INSERT[/green] {detail.name!r}"
                    f" (touhoudb_id={detail.id}, {work_str},"
                    f" stage={stage}, characters={char_count})"
                )
                stats["upserted"] += 1
                stats["characters_linked"] += char_count
                if work_id is None:
                    stats["no_work"] += 1
    else:
        progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        )
        with progress:
            upsert_task = progress.add_task("Upserting original songs…", total=len(all_songs))

            # Each song is committed in its own transaction so that:
            # - Supabase's statement timeout cannot kill the entire batch
            # - Progress is durable as it goes (no all-or-nothing rollback)
            for detail in all_songs:
                with engine.begin() as conn:
                    work_id = match_work_for_song(detail.albums, conn)
                    if work_id is None:
                        stats["no_work"] += 1

                    original_song_id = upsert_original_song(detail, work_id, conn)
                    stats["upserted"] += 1

                    chars = link_original_song_characters(original_song_id, detail, conn)
                    stats["characters_linked"] += chars

                progress.advance(upsert_task)

        # Task resolution runs in a single separate transaction after all songs
        # are committed — link_song_originals needs the rows to be visible.
        console.print("Resolving FILL_MISSING_INFO tasks…")
        with engine.begin() as conn:
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
        f"  Original songs upserted  : {stats['upserted']}\n"
        f"  Characters linked        : {stats['characters_linked']}\n"
        f"  Songs without work match : {stats['no_work']}\n"
        f"  Tasks resolved           : {stats['tasks_resolved']}"
    )


# ---------------------------------------------------------------------------
# Enrich command — Touhou Wiki backfill of stage / is_boss / characters
# ---------------------------------------------------------------------------


@originals.command("enrich")
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print intended updates without writing to the database.",
)
@click.option(
    "--game",
    "only_game",
    default=None,
    type=str,
    help=(
        "Run for a single game only — accepts a wiki slug "
        "(e.g. Embodiment_of_Scarlet_Devil) or a works.short_name (e.g. EoSD)."
    ),
)
@click.option(
    "--no-spell-cards",
    is_flag=True,
    default=False,
    help=(
        "Skip per-stage spell-card fetches — heading-derived enrichment only. "
        "Faster and avoids any MEDIUM-confidence character links."
    ),
)
def enrich(dry_run: bool, only_game: str | None, no_spell_cards: bool) -> None:
    """Backfill stage / is_boss / characters on original_songs from Touhou Wiki.

    Walks ``List_by_Song/<game>`` headings (and per-stage spell-card pages)
    to fill in fields TouhouDB does not provide:

    \b
      - original_songs.stage         (TouhouDB tags cover ~30% of songs)
      - original_songs.is_boss       (TouhouDB does not differentiate)
      - original_song_characters     (HIGH from boss-theme headings,
                                      MEDIUM from stage spell-card owners)

    Also opportunistically upserts ``character_works`` rows for boss/extra
    encounters — feeding the Spec §M5 character_works-driven rule for free.

    Stage-theme character mappings always surface a REVIEW_CHARACTER_MAPPING
    task so the user can confirm/correct (e.g. add midbosses without spell
    cards like Lily White / Daiyousei).
    """
    asyncio.run(_run_enrich(dry_run=dry_run, only_game=only_game, no_spell_cards=no_spell_cards))


async def _run_enrich(*, dry_run: bool, only_game: str | None, no_spell_cards: bool) -> None:
    settings = get_settings()
    engine = get_engine()

    games = _select_games(only_game)
    if not games:
        raise click.BadParameter(f"Unknown game: {only_game!r}")

    if dry_run:
        console.print("[yellow]DRY RUN — no changes will be written[/yellow]")
    if no_spell_cards:
        console.print("[dim]Spell-card lookups disabled (--no-spell-cards)[/dim]")

    totals: dict[str, int] = {
        "themes_matched": 0,
        "themes_unmatched": 0,
        "stage_updates": 0,
        "is_boss_updates": 0,
        "char_links_high": 0,
        "char_links_medium": 0,
        "char_unresolved": 0,
        "tasks_review_character": 0,
        "tasks_fill_missing": 0,
        "games_processed": 0,
        "games_skipped_no_work": 0,
        "games_skipped_no_page": 0,
    }

    async with TouhouWikiClient.from_settings(settings) as client:
        for game in games:
            console.rule(f"[bold cyan]{game.works_short_name}[/bold cyan]  ({game.wiki_slug})")
            game_stats = await _enrich_one_game(
                client=client,
                game=game,
                engine=engine,
                dry_run=dry_run,
                no_spell_cards=no_spell_cards,
            )
            for key, value in game_stats.items():
                totals[key] = totals.get(key, 0) + value

    _print_enrich_summary(totals, dry_run=dry_run)


def _select_games(only_game: str | None) -> list[WikiGame]:
    if only_game is None:
        return list(GAMES)
    if only_game in GAMES_BY_SLUG:
        return [GAMES_BY_SLUG[only_game]]
    if only_game in GAMES_BY_SHORT_NAME:
        return [GAMES_BY_SHORT_NAME[only_game]]
    return []


async def _enrich_one_game(
    *,
    client: TouhouWikiClient,
    game: WikiGame,
    engine: sa.Engine,
    dry_run: bool,
    no_spell_cards: bool,
) -> dict[str, int]:
    stats: dict[str, int] = {
        "themes_matched": 0,
        "themes_unmatched": 0,
        "stage_updates": 0,
        "is_boss_updates": 0,
        "char_links_high": 0,
        "char_links_medium": 0,
        "char_unresolved": 0,
        "tasks_review_character": 0,
        "tasks_fill_missing": 0,
        "games_processed": 0,
        "games_skipped_no_work": 0,
        "games_skipped_no_page": 0,
    }

    # Resolve the work_id once per game (read-only).
    with engine.connect() as conn:
        work_id = get_work_by_short_name(game.works_short_name, conn)
    if work_id is None:
        console.print(
            f"  [yellow]skipped[/yellow]: no works row for short_name={game.works_short_name!r}"
        )
        stats["games_skipped_no_work"] = 1
        return stats

    sections = await client.get_song_listing(game.wiki_slug)
    if sections is None:
        console.print("  [yellow]skipped[/yellow]: wiki page not found")
        stats["games_skipped_no_page"] = 1
        return stats

    stats["games_processed"] = 1

    # Build the candidate pool once per game.
    with engine.connect() as conn:
        candidates = list_originals_for_work(work_id, conn)

    # Per-stage cache so we only fetch each spell-card page at most once,
    # even if multiple non-boss themes share the same stage (rare, but
    # cheap insurance).
    spell_card_owners_cache: dict[int, list[str] | None] = {}

    for heading_line, body in sections.items():
        parsed = parse_section_heading(heading_line, final_stage_number=game.final_stage_number)
        if parsed is None:
            continue

        japanese_title = extract_japanese_title(body)
        if japanese_title is None:
            # No bold name under the heading — usually a placeholder section
            # or a heading with no theme below it.  Skip silently.
            continue

        original_song_id = match_original_song(candidates, japanese_title)
        if original_song_id is None:
            stats["themes_unmatched"] += 1
            console.print(f"  [red]?[/red] {heading_line!r} — no match for {japanese_title!r}")
            if not dry_run:
                with engine.begin() as conn:
                    if create_fill_missing_info_task(
                        title=(
                            f"Wiki theme not in DB: {japanese_title!r} ({game.works_short_name})"
                        ),
                        data={
                            "work_id": work_id,
                            "work_short_name": game.works_short_name,
                            "wiki_slug": game.wiki_slug,
                            "wiki_heading": heading_line,
                            "japanese_title": japanese_title,
                            "stage": parsed.stage,
                            "is_boss": parsed.is_boss,
                            "wiki_character_name": parsed.character_name,
                            "source": "touhouwiki_enrich",
                        },
                        conn=conn,
                    ):
                        stats["tasks_fill_missing"] += 1
            continue

        stats["themes_matched"] += 1

        if dry_run:
            char_str = f" character={parsed.character_name!r}" if parsed.character_name else ""
            midboss_str = " (midboss)" if parsed.is_midboss else ""
            console.print(
                f"  [green]match[/green] {japanese_title!r} → "
                f"stage={parsed.stage} is_boss={parsed.is_boss}{char_str}{midboss_str}"
            )
            if parsed.character_name:
                stats["char_links_high"] += 1
        else:
            with engine.begin() as conn:
                stage_changed, is_boss_changed = update_original_song_stage_boss(
                    original_song_id, parsed.stage, parsed.is_boss, conn
                )
                if stage_changed:
                    stats["stage_updates"] += 1
                if is_boss_changed:
                    stats["is_boss_updates"] += 1

                if parsed.character_name:
                    character_id = match_character_by_name(parsed.character_name, conn)
                    if character_id is None:
                        stats["char_unresolved"] += 1
                        console.print(
                            f"  [red]?[/red] character not in DB: "
                            f"{parsed.character_name!r} ({heading_line})"
                        )
                    else:
                        outcome = link_character_to_original(
                            original_song_id,
                            character_id,
                            ConfidenceLevel.HIGH,
                            conn,
                        )
                        if outcome in {"inserted", "upgraded"}:
                            stats["char_links_high"] += 1
                        if parsed.is_boss:
                            upsert_character_work(character_id, work_id, AppearanceType.BOSS, conn)
                        elif parsed.is_midboss:
                            upsert_character_work(
                                character_id, work_id, AppearanceType.MIDBOSS, conn
                            )

        # Spell-card-derived stage-theme characters — only for non-boss
        # stage themes on games with the spell-card system.
        is_stage_theme = 1 <= parsed.stage <= 7 and not parsed.is_boss and not parsed.is_midboss
        if is_stage_theme and game.has_spell_cards and not no_spell_cards:
            await _link_spell_card_owners(
                client=client,
                game=game,
                engine=engine,
                original_song_id=original_song_id,
                work_id=work_id,
                stage=parsed.stage,
                heading_line=heading_line,
                japanese_title=japanese_title,
                cache=spell_card_owners_cache,
                stats=stats,
                dry_run=dry_run,
            )

    console.print(
        f"  [green]done[/green]: matched={stats['themes_matched']} "
        f"unmatched={stats['themes_unmatched']} "
        f"stage_updates={stats['stage_updates']} "
        f"is_boss_updates={stats['is_boss_updates']} "
        f"chars_HIGH={stats['char_links_high']} "
        f"chars_MEDIUM={stats['char_links_medium']}"
    )
    return stats


async def _link_spell_card_owners(
    *,
    client: TouhouWikiClient,
    game: WikiGame,
    engine: sa.Engine,
    original_song_id: int,
    work_id: int,
    stage: int,
    heading_line: str,
    japanese_title: str,
    cache: dict[int, list[str] | None],
    stats: dict[str, int],
    dry_run: bool,
) -> None:
    """Fetch spell-card owners for a stage and link them at MEDIUM confidence.

    Always raises a REVIEW_CHARACTER_MAPPING task per the M5 plan, even when
    spell-card owners resolve cleanly: midbosses without spell cards (Lily
    White, Daiyousei, etc.) won't appear via the API and need manual review.
    """
    if stage not in cache:
        cache[stage] = await client.get_spell_card_owners(game.wiki_slug, stage)
    owner_names = cache[stage]
    if owner_names is None:
        return

    # Dedupe while preserving first-seen order for stable task data payloads.
    seen: set[str] = set()
    unique_owners: list[str] = []
    for n in owner_names:
        if n not in seen:
            seen.add(n)
            unique_owners.append(n)

    candidate_ids: list[tuple[str, int | None]] = []
    if not dry_run:
        with engine.begin() as conn:
            for owner in unique_owners:
                cid = match_character_by_name(owner, conn)
                candidate_ids.append((owner, cid))
                if cid is None:
                    stats["char_unresolved"] += 1
                    continue
                outcome = link_character_to_original(
                    original_song_id, cid, ConfidenceLevel.MEDIUM, conn
                )
                if outcome in {"inserted", "upgraded"}:
                    stats["char_links_medium"] += 1

            inserted = create_review_character_mapping_task(
                title=(
                    f"Review characters for {japanese_title!r} "
                    f"({game.works_short_name} stage {stage})"
                ),
                data={
                    "original_song_id": original_song_id,
                    "work_id": work_id,
                    "work_short_name": game.works_short_name,
                    "wiki_slug": game.wiki_slug,
                    "wiki_heading": heading_line,
                    "japanese_title": japanese_title,
                    "stage": stage,
                    "spell_card_owners": unique_owners,
                    "note": (
                        "Spell-card owners include both boss and midbosses; "
                        "midbosses without spell cards (e.g. Lily White, Daiyousei) "
                        "are not captured automatically and may need to be added."
                    ),
                    "source": "touhouwiki_enrich",
                },
                conn=conn,
            )
            if inserted:
                stats["tasks_review_character"] += 1
    else:
        # Dry-run summary line per stage theme.
        stats["char_links_medium"] += len(unique_owners)
        console.print(f"  [dim]stage {stage}[/dim] {japanese_title!r} → owners={unique_owners!r}")


def _print_enrich_summary(stats: dict[str, int], *, dry_run: bool) -> None:
    prefix = "[yellow]DRY RUN[/yellow] — " if dry_run else ""
    console.print(
        f"\n{prefix}[bold]Enrich done.[/bold]\n"
        f"  Games processed              : {stats['games_processed']}\n"
        f"  Games skipped (no work row)  : {stats['games_skipped_no_work']}\n"
        f"  Games skipped (no wiki page) : {stats['games_skipped_no_page']}\n"
        f"  Themes matched               : {stats['themes_matched']}\n"
        f"  Themes unmatched             : {stats['themes_unmatched']}\n"
        f"  Stage updates                : {stats['stage_updates']}\n"
        f"  is_boss updates              : {stats['is_boss_updates']}\n"
        f"  Character links (HIGH)       : {stats['char_links_high']}\n"
        f"  Character links (MEDIUM)     : {stats['char_links_medium']}\n"
        f"  Unresolved character names   : {stats['char_unresolved']}\n"
        f"  REVIEW_CHARACTER_MAPPING tasks: {stats['tasks_review_character']}\n"
        f"  FILL_MISSING_INFO tasks      : {stats['tasks_fill_missing']}"
    )
