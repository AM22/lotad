"""TouhouDB contribution CLI commands."""

from __future__ import annotations

# Import command modules to trigger @contrib.command() registration.
from lotad.cli.contrib import add_videos, login, status  # noqa: F401
from lotad.cli.contrib._group import contrib

__all__ = ["contrib"]
