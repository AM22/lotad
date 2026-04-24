"""tasks resolve command."""

from __future__ import annotations

import asyncio

import click

from lotad.cli.tasks._group import tasks
from lotad.cli.tasks._shared import console
from lotad.cli.tasks._wizards import (
    _resolve_deduplicate_songs,
    _resolve_dropped_video,
    _resolve_fill_missing_info,
    _resolve_generic,
    _resolve_ingest_failed,
    _resolve_missing_lyricist,
    _resolve_suspicious_metadata,
)
from lotad.db.models import TaskStatus, TaskType
from lotad.db.session import get_engine
from lotad.tasks import manager


@tasks.command("resolve")
@click.argument("task_id", type=int)
def tasks_resolve(task_id: int) -> None:
    """Interactively resolve a task."""
    engine = get_engine()
    with engine.connect() as conn:
        ctx = manager.get_task_with_context(conn, task_id)

    if ctx is None:
        console.print(f"[red]Task #{task_id} not found.[/red]")
        raise click.Abort()

    task = ctx["task"]
    tt = str(task["task_type"])

    if str(task["status"]) == TaskStatus.RESOLVED:
        console.print(f"[green]Task #{task_id} is already resolved.[/green]")
        return
    if str(task["status"]) == TaskStatus.DISMISSED:
        console.print(f"[dim]Task #{task_id} has been dismissed.[/dim]")
        return

    if tt == TaskType.INGEST_FAILED:
        asyncio.run(_resolve_ingest_failed(task_id, ctx))
    elif tt == TaskType.SUSPICIOUS_METADATA:
        _resolve_suspicious_metadata(task_id, ctx)
    elif tt == TaskType.DEDUPLICATE_SONGS:
        _resolve_deduplicate_songs(task_id, ctx)
    elif tt == TaskType.MISSING_LYRICIST:
        _resolve_missing_lyricist(task_id, ctx)
    elif tt == TaskType.DROPPED_VIDEO:
        _resolve_dropped_video(task_id, ctx)
    elif tt == TaskType.FILL_MISSING_INFO:
        _resolve_fill_missing_info(task_id, ctx)
    else:
        _resolve_generic(task_id, ctx)
