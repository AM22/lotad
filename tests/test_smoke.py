"""Smoke tests — verify the package imports and CLI entry point are wired up."""

import importlib


def test_lotad_package_importable():
    """The lotad package must be importable without errors."""
    mod = importlib.import_module("lotad")
    assert mod is not None


def test_cli_importable():
    """The CLI entry point must be importable."""
    from lotad.cli.main import cli

    assert cli is not None


def test_config_importable():
    """Config module must import (Settings validation is skipped when no .env)."""
    import lotad.config  # noqa: F401
