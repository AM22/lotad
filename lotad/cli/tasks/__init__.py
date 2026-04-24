"""Task management CLI commands."""

from __future__ import annotations

# Import command modules to trigger @tasks.command() registration.
# These imports appear after _group to satisfy isort's known-first-party grouping;
# circular-import safety comes from command files importing from _group, not __init__.
from lotad.cli.tasks import dismiss, enrich, list_, resolve, show  # noqa: F401
from lotad.cli.tasks._group import tasks

__all__ = ["tasks"]
