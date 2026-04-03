"""LOTAD CLI entry point."""

import click

from lotad.cli.db import db
from lotad.cli.ingest import ingest
from lotad.cli.score import score
from lotad.cli.sync import sync
from lotad.cli.tasks import tasks


@click.group()
@click.version_option()
def cli() -> None:
    """LOTAD — Local Ordered Touhou Arrangements Database."""


cli.add_command(db)
cli.add_command(ingest)
cli.add_command(tasks)
cli.add_command(score)
cli.add_command(sync)
