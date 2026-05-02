"""lotad contrib status — verify the persisted TouhouDB session is still valid."""

from __future__ import annotations

import asyncio
from pathlib import Path

import click
from rich.console import Console

from lotad.cli.contrib._group import contrib
from lotad.config import get_settings
from lotad.contrib.touhoudb_session import (
    AuthenticationError,
    SessionNotLoaded,
    TouhouDBSession,
)

console = Console()


@contrib.command("status")
def status_cmd() -> None:
    """Show whether the persisted TouhouDB session is still valid."""
    settings = get_settings()
    session_path = Path(settings.touhoudb_session_path)
    asyncio.run(_run_status(session_path))


async def _run_status(session_path: Path) -> None:
    settings = get_settings()
    async with TouhouDBSession.from_settings(settings) as session:
        try:
            session.load(session_path)
            verified = await session.verify()
        except SessionNotLoaded as exc:
            console.print(f"[yellow]No active session:[/yellow] {exc}")
            raise click.exceptions.Exit(1) from None
        except AuthenticationError as exc:
            console.print(
                f"[red]Session expired or invalid:[/red] {exc}\n"
                f"Run [bold]lotad contrib login[/bold] to re-authenticate."
            )
            raise click.exceptions.Exit(1) from None
    console.print(f"[green]✓ Logged in as [bold]{verified}[/bold].[/green]")
