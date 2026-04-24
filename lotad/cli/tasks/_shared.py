"""Shared helpers, constants, and formatters used across task sub-commands."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from rich.console import Console

from lotad.db.models import TaskType

console = Console()
logger = logging.getLogger(__name__)

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

_CONFIDENCE_COLOR: dict[str, str] = {"HIGH": "green", "MEDIUM": "yellow", "LOW": "red"}

_STATUS_COLOR: dict[str, str] = {
    "OPEN": "yellow",
    "RESOLVED": "green",
    "DISMISSED": "dim",
    "IN_PROGRESS": "blue",
}


def _age(created_at: Any) -> str:
    if created_at is None:
        return "?"
    if isinstance(created_at, str):
        try:
            created_at = datetime.fromisoformat(created_at)
        except ValueError:
            return "?"
    now = datetime.now(UTC)
    # SQLAlchemy may return naive datetimes from the DB; assume UTC
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


def _get_data(task_row: Any) -> dict[str, Any]:
    raw = task_row["data"]
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return {}
    return raw or {}


def _llm_status_cell(row: Any) -> str:
    """Return a Rich-formatted string for the LLM column in `tasks list`."""
    if not row.get("llm_enriched_at"):
        return "—"
    data = _get_data(row)
    llm_match = data.get("llm_match")
    if llm_match:
        conf = llm_match.get("confidence", "")
        color = _CONFIDENCE_COLOR.get(conf, "white")
        return f"[{color}]{conf}[/{color}]"
    if data.get("llm_classification"):
        return "[red]no match[/red]"
    return "[green]enriched[/green]"
