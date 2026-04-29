"""lotad sync — sync commands (stub)."""

from __future__ import annotations

import click


@click.group()
def sync() -> None:
    """Sync playlists and TouhouDB metadata."""
