"""lotad contrib login — authenticate against TouhouDB and persist the session."""

from __future__ import annotations

import asyncio
import getpass
from pathlib import Path

import click
from rich.console import Console

from lotad.cli.contrib._group import contrib
from lotad.config import get_settings
from lotad.contrib.touhoudb_session import AuthenticationError, TouhouDBSession

console = Console()


@contrib.command("login")
@click.option(
    "--username",
    "-u",
    default=None,
    help="TouhouDB username. Defaults to TOUHOUDB_USERNAME env var.",
)
def login_cmd(username: str | None) -> None:
    """Log in to TouhouDB and persist the session cookie to disk.

    The password is prompted interactively (no echo) and never stored.
    Subsequent `lotad contrib …` commands reuse the saved session until it
    expires; re-run this command at that point.
    """
    settings = get_settings()
    user = username or settings.touhoudb_username
    if not user:
        raise click.UsageError(
            "No username provided. Pass --username or set TOUHOUDB_USERNAME in .env."
        )
    password = getpass.getpass(f"TouhouDB password for {user}: ")
    if not password:
        raise click.UsageError("Empty password; aborting.")

    asyncio.run(_run_login(user, password, Path(settings.touhoudb_session_path)))


async def _run_login(username: str, password: str, session_path: Path) -> None:
    settings = get_settings()
    async with TouhouDBSession.from_settings(settings) as session:
        try:
            await session.login(username, password)
            await session.fetch_antiforgery_token()
            verified = await session.verify()
        except AuthenticationError as exc:
            console.print(f"[red]Login failed:[/red] {exc}")
            raise click.exceptions.Exit(1) from None
        session.save(session_path)
    console.print(
        f"[green]✓ Logged in as [bold]{verified}[/bold].[/green] "
        f"Session persisted to {session_path}"
    )
