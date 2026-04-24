"""tasks list command."""

from __future__ import annotations

import click
from rich.panel import Panel
from rich.table import Table

from lotad.cli.tasks._group import tasks
from lotad.cli.tasks._shared import _TYPE_ORDER, _age, _llm_status_cell, console
from lotad.db.models import TaskStatus, TaskType
from lotad.db.session import get_engine
from lotad.tasks import manager


@tasks.command("list")
@click.option("--type", "task_type", default=None, help="Filter by task type.")
@click.option(
    "--status",
    "status_str",
    default="OPEN",
    show_default=True,
    help="Filter by status (OPEN, RESOLVED, DISMISSED, IN_PROGRESS, all).",
)
@click.option("--limit", default=50, show_default=True, help="Max tasks shown per type.")
def tasks_list(task_type: str | None, status_str: str, limit: int) -> None:
    """List tasks grouped by type."""
    status: TaskStatus | None = None
    if status_str.lower() != "all":
        try:
            status = TaskStatus(status_str.upper())
        except ValueError:
            console.print(f"[red]Unknown status: {status_str!r}[/red]")
            raise click.Abort() from None

    parsed_type: TaskType | None = None
    if task_type:
        try:
            parsed_type = TaskType(task_type.upper())
        except ValueError:
            console.print(f"[red]Unknown task type: {task_type!r}[/red]")
            raise click.Abort() from None

    engine = get_engine()
    with engine.connect() as conn:
        counts = manager.count_tasks_by_type(conn, status=status)
        if not counts:
            console.print("[green]No tasks found.[/green]")
            return

        total = sum(counts.values())
        console.print(f"\n[bold]Tasks — {total} {status_str.lower()}[/bold]\n")

        type_list = [parsed_type] if parsed_type else _TYPE_ORDER
        for tt in type_list:
            count = counts.get(tt, 0)
            if count == 0:
                continue

            rows = manager.list_tasks(conn, task_type=tt, status=status, limit=limit)

            table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
            table.add_column("ID", style="cyan", width=6)
            table.add_column("Title", no_wrap=False, max_width=55)
            table.add_column("Age", width=9)
            if tt == TaskType.INGEST_FAILED:
                table.add_column("LLM", width=10)

            for row in rows:
                if tt == TaskType.INGEST_FAILED:
                    llm_cell = _llm_status_cell(row)
                    table.add_row(str(row["id"]), row["title"], _age(row["created_at"]), llm_cell)
                else:
                    table.add_row(str(row["id"]), row["title"], _age(row["created_at"]))

            suffix = f" ({limit} of {count} shown)" if count > limit else ""
            title_str = f"[bold]{tt.value}[/bold] ({count}){suffix}"

            if tt == TaskType.INGEST_FAILED:
                unenriched = sum(1 for r in rows if not r.get("llm_enriched_at"))
                if unenriched > 0:
                    subtitle = (
                        f"[dim]{unenriched} unenriched"
                        " — run `lotad tasks enrich --limit 100`[/dim]"
                    )
                    console.print(
                        Panel(table, title=title_str, border_style="blue", subtitle=subtitle)
                    )
                    continue

            console.print(Panel(table, title=title_str, border_style="blue"))

        console.print()
