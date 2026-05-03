"""DB helpers for the ``lotad originals enrich`` flow.

Kept separate from ``mappers.py`` so the wiki-driven enrichment logic can
evolve without touching the TouhouDB-driven scrape path.

Functions here are sync (called inside ``engine.begin()`` blocks) — only the
HTTP layer is async.
"""

from __future__ import annotations

import difflib
import logging
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Connection
from sqlalchemy.dialects.postgresql import insert as pg_insert

from lotad.db.models import (
    AppearanceType,
    ConfidenceLevel,
    TaskStatus,
    TaskType,
    character_works,
    characters,
    original_song_characters,
    original_songs,
    tasks,
    works,
)
from lotad.ingestion.touhouwiki_parser import (
    name_token_set,
    normalize_japanese_title,
)

logger = logging.getLogger(__name__)

# Confidence weights used to decide whether a new row should overwrite an
# existing ``original_song_characters`` link.  We never demote: a manually
# resolved HIGH link must not be downgraded to MEDIUM by a subsequent
# spell-card-derived run.
_CONFIDENCE_RANK: dict[str, int] = {
    ConfidenceLevel.LOW.value: 0,
    ConfidenceLevel.MEDIUM.value: 1,
    ConfidenceLevel.HIGH.value: 2,
}

# Threshold for the difflib fallback when matching a wiki theme title to an
# ``original_songs.name``.  Tighter than the album-matching threshold (0.6)
# in mappers.py — theme names are short, so noisier comparisons aren't safe.
_TITLE_SIMILARITY_THRESHOLD = 0.85


def get_work_by_short_name(short_name: str, conn: Connection) -> int | None:
    row = conn.execute(sa.select(works.c.id).where(works.c.short_name == short_name)).one_or_none()
    return row[0] if row else None


def list_originals_for_work(work_id: int, conn: Connection) -> list[Any]:
    """Return all ``original_songs`` rows for a work — used as the match pool."""
    return list(
        conn.execute(
            sa.select(
                original_songs.c.id,
                original_songs.c.name,
                original_songs.c.name_romanized,
                original_songs.c.stage,
                original_songs.c.is_boss,
            ).where(original_songs.c.work_id == work_id)
        ).fetchall()
    )


def match_original_song(
    candidates: list[Any],
    japanese_title: str,
) -> int | None:
    """Find the ``original_songs.id`` whose name matches ``japanese_title``.

    Two-pass matcher:

    1. NFKC + tilde-normalized exact equality against ``name``.
    2. ``difflib.SequenceMatcher`` ratio ≥ 0.85 against the same normalized
       names; ties broken by max ratio.

    Returns ``None`` if nothing reaches the threshold (caller surfaces a task).
    """
    target = normalize_japanese_title(japanese_title)
    if not target:
        return None

    # Pass 1: exact normalized match
    for row in candidates:
        if normalize_japanese_title(row.name) == target:
            return int(row.id)

    # Pass 2: difflib fallback
    best_id: int | None = None
    best_ratio = 0.0
    for row in candidates:
        ratio = difflib.SequenceMatcher(None, target, normalize_japanese_title(row.name)).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_id = int(row.id)
    if best_ratio >= _TITLE_SIMILARITY_THRESHOLD:
        return best_id
    return None


def match_character_by_name(name: str, conn: Connection) -> int | None:
    """Resolve a romanized character name to ``characters.id``.

    Order:

    1. Case-insensitive exact match on ``characters.name_romanized``.
    2. Case-insensitive exact match against any element of
       ``characters.other_names``.
    3. Token-set (Jaccard 1.0) match on ``name_romanized`` — tolerates
       Western vs. Japanese name order (``Sakuya Izayoi`` vs.
       ``Izayoi Sakuya``).
    """
    target = name.strip()
    if not target:
        return None

    rows = conn.execute(
        sa.select(characters.c.id, characters.c.name_romanized, characters.c.other_names)
    ).fetchall()

    target_lower = target.lower()
    for row in rows:
        if row.name_romanized and row.name_romanized.lower() == target_lower:
            return int(row.id)

    for row in rows:
        for other in row.other_names or []:
            if other and other.lower() == target_lower:
                return int(row.id)

    target_tokens = name_token_set(target)
    if not target_tokens:
        return None
    for row in rows:
        if row.name_romanized and name_token_set(row.name_romanized) == target_tokens:
            return int(row.id)
    return None


def update_original_song_stage_boss(
    original_song_id: int,
    stage: int,
    is_boss: bool,
    conn: Connection,
) -> tuple[bool, bool]:
    """Update ``stage`` / ``is_boss`` on one row.

    Returns ``(stage_changed, is_boss_changed)`` so the caller can produce
    accurate per-game stats.  The wiki is treated as authoritative — both
    fields are overwritten even when previously set, since TouhouDB-derived
    stage tags cover only ~30% of songs and is_boss is currently always False.
    """
    existing = conn.execute(
        sa.select(original_songs.c.stage, original_songs.c.is_boss).where(
            original_songs.c.id == original_song_id
        )
    ).one()
    stage_changed = existing.stage != stage
    is_boss_changed = bool(existing.is_boss) != bool(is_boss)
    if stage_changed or is_boss_changed:
        conn.execute(
            original_songs.update()
            .where(original_songs.c.id == original_song_id)
            .values(stage=stage, is_boss=is_boss)
        )
    return stage_changed, is_boss_changed


def link_character_to_original(
    original_song_id: int,
    character_id: int,
    confidence: ConfidenceLevel,
    conn: Connection,
) -> str:
    """Insert or upgrade a character link.

    Returns one of: ``"inserted"`` (new row), ``"upgraded"`` (existing row's
    confidence raised), ``"unchanged"`` (already at this level or higher).

    Never demotes — a HIGH link is preserved across re-runs even if the
    wiki only supports MEDIUM the second time around.
    """
    existing = conn.execute(
        sa.select(original_song_characters.c.confidence).where(
            sa.and_(
                original_song_characters.c.original_song_id == original_song_id,
                original_song_characters.c.character_id == character_id,
            )
        )
    ).one_or_none()

    if existing is None:
        conn.execute(
            pg_insert(original_song_characters).values(
                original_song_id=original_song_id,
                character_id=character_id,
                confidence=confidence,
            )
        )
        return "inserted"

    existing_value = (
        existing.confidence.value
        if hasattr(existing.confidence, "value")
        else str(existing.confidence)
    )
    if _CONFIDENCE_RANK[confidence.value] > _CONFIDENCE_RANK[existing_value]:
        conn.execute(
            original_song_characters.update()
            .where(
                sa.and_(
                    original_song_characters.c.original_song_id == original_song_id,
                    original_song_characters.c.character_id == character_id,
                )
            )
            .values(confidence=confidence)
        )
        return "upgraded"
    return "unchanged"


def upsert_character_work(
    character_id: int,
    work_id: int,
    appearance_type: AppearanceType,
    conn: Connection,
) -> None:
    """Best-effort: ensure the ``character_works`` row exists.

    Skipped if it already does; no demotion logic — the row's primary key
    already encodes the appearance type, so we don't need to compare.
    """
    conn.execute(
        pg_insert(character_works)
        .values(
            character_id=character_id,
            work_id=work_id,
            appearance_type=appearance_type,
        )
        .on_conflict_do_nothing()
    )


def create_review_character_mapping_task(
    *,
    title: str,
    data: dict[str, Any],
    conn: Connection,
) -> bool:
    """Create a ``REVIEW_CHARACTER_MAPPING`` task, deduping by ``original_song_id``.

    These tasks have no ``related_song_id`` (which is a FK to ``songs``, not
    ``original_songs``) so the partial unique index doesn't apply — we dedupe
    manually by inspecting OPEN tasks of this type that target the same
    ``original_song_id`` in their ``data`` payload.

    Returns True if a new row was inserted, False if an open task already existed.
    """
    target_id = data.get("original_song_id")
    if target_id is not None:
        existing = conn.execute(
            sa.select(tasks.c.id).where(
                sa.and_(
                    tasks.c.task_type == TaskType.REVIEW_CHARACTER_MAPPING,
                    tasks.c.status == TaskStatus.OPEN,
                    tasks.c.data["original_song_id"].as_integer() == int(target_id),
                )
            )
        ).first()
        if existing:
            conn.execute(
                tasks.update().where(tasks.c.id == existing[0]).values(title=title, data=data)
            )
            return False

    conn.execute(
        tasks.insert().values(
            task_type=TaskType.REVIEW_CHARACTER_MAPPING,
            title=title,
            data=data,
            auto_created_by="originals_enrich",
        )
    )
    return True


def create_fill_missing_info_task(
    *,
    title: str,
    data: dict[str, Any],
    conn: Connection,
) -> bool:
    """Surface an unmatched theme heading for manual lookup.

    Used when a wiki section's bold Japanese title does not match any
    existing ``original_songs.name`` for the work.  Dedupes per
    (work_id, japanese_title) tuple from the data payload.
    """
    work_id = data.get("work_id")
    japanese_title = data.get("japanese_title")
    if work_id is not None and japanese_title is not None:
        existing = conn.execute(
            sa.select(tasks.c.id).where(
                sa.and_(
                    tasks.c.task_type == TaskType.FILL_MISSING_INFO,
                    tasks.c.status == TaskStatus.OPEN,
                    tasks.c.data["work_id"].as_integer() == int(work_id),
                    tasks.c.data["japanese_title"].astext == japanese_title,
                )
            )
        ).first()
        if existing:
            conn.execute(
                tasks.update().where(tasks.c.id == existing[0]).values(title=title, data=data)
            )
            return False

    conn.execute(
        tasks.insert().values(
            task_type=TaskType.FILL_MISSING_INFO,
            title=title,
            data=data,
            auto_created_by="originals_enrich",
        )
    )
    return True
