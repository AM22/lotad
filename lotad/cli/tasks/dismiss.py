"""tasks dismiss and bulk-dismiss commands."""

from __future__ import annotations

import click

from lotad.cli.tasks._group import tasks
from lotad.cli.tasks._shared import console
from lotad.db.models import TaskStatus, TaskType
from lotad.db.session import get_engine
from lotad.tasks import manager


@tasks.command("dismiss")
@click.argument("task_id", type=int)
@click.option("--note", default=None, help="Optional note explaining the dismissal.")
def tasks_dismiss(task_id: int, note: str | None) -> None:
    """Dismiss a task (mark as false positive or permanently skip)."""
    engine = get_engine()
    with engine.begin() as conn:
        task_row = manager.get_task(conn, task_id)
        if task_row is None:
            console.print(f"[red]Task #{task_id} not found.[/red]")
            raise click.Abort()
        manager.dismiss_task(conn, task_id, note=note)
    console.print(f"[green]Dismissed task #{task_id}.[/green]")


@tasks.command("bulk-dismiss")
@click.option("--type", "task_type_str", required=True, help="Task type to bulk-dismiss.")
@click.option("--yes", is_flag=True, default=False, help="Confirm the operation (required).")
def tasks_bulk_dismiss(task_type_str: str, yes: bool) -> None:
    """Bulk-dismiss all OPEN tasks of a given type."""
    try:
        task_type = TaskType(task_type_str.upper())
    except ValueError:
        console.print(f"[red]Unknown task type: {task_type_str!r}[/red]")
        raise click.Abort() from None

    engine = get_engine()
    with engine.connect() as conn:
        counts = manager.count_tasks_by_type(conn, status=TaskStatus.OPEN)
        count = counts.get(task_type, 0)

    if count == 0:
        console.print(f"No OPEN {task_type.value} tasks to dismiss.")
        return

    console.print(f"This will dismiss [bold]{count}[/bold] OPEN {task_type.value} tasks.")
    if not yes:
        console.print("[yellow]Use --yes to confirm.[/yellow]")
        raise click.Abort()

    with engine.begin() as conn:
        dismissed = manager.bulk_dismiss_by_type(conn, task_type)
    console.print(f"[green]Dismissed {dismissed} {task_type.value} tasks.[/green]")
