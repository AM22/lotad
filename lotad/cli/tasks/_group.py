"""Click group definition for the tasks sub-CLI.

Isolated in its own module so command files can import `tasks` without
triggering a circular import through __init__.py.
"""

from __future__ import annotations

import click


@click.group()
def tasks() -> None:
    """Manage human-in-the-loop review tasks."""
