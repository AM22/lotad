"""LOTAD CLI entry point."""

import click

from lotad.cli.ingest import ingest
from lotad.cli.tasks import tasks
from lotad.cli.score import score
from lotad.cli.sync import sync


@click.group()
@click.version_option()
def cli() -> None:
    """LOTAD — Local Ordered Touhou Arrangements Database."""


cli.add_command(ingest)
cli.add_command(tasks)
cli.add_command(score)
cli.add_command(sync)
