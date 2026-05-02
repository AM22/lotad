"""Click group definition for the contrib sub-CLI.

Isolated in its own module so command files can import `contrib` without
triggering a circular import through __init__.py (mirrors lotad/cli/tasks/).
"""

from __future__ import annotations

import click


@click.group()
def contrib() -> None:
    """Submit drafts back to TouhouDB on behalf of the operator."""
