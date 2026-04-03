"""Database management commands: migrations and seeding."""

from __future__ import annotations

import click


@click.group()
def db() -> None:
    """Database management (migrations, seeding)."""


@db.command("seed-playlists")
def seed_playlists() -> None:
    """Seed playlists and scoring configurations."""
    from lotad.db.seeds.playlists import seed

    seed()


@db.command("seed-works")
def seed_works() -> None:
    """Seed the works table with canonical Touhou games, music CDs, and books."""
    from lotad.db.seeds.works import seed

    seed()


@db.command("seed-all")
def seed_all() -> None:
    """Run all seed scripts in dependency order."""
    from lotad.db.seeds.works import seed as seed_works_fn
    from lotad.db.seeds.playlists import seed as seed_playlists_fn

    seed_works_fn()
    seed_playlists_fn()
