"""Alembic environment configuration.

Reads the database URL from the LOTAD settings (which in turn reads from
.env or environment variables) so no DSN is ever hard-coded.
"""

from __future__ import annotations

from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context

# ---------------------------------------------------------------------------
# Import the metadata object that holds all table definitions.
# This is populated by lotad/db/models.py (Milestone 1).
# ---------------------------------------------------------------------------
try:
    from lotad.db.models import metadata as target_metadata
except ImportError:
    # models.py not yet created — Alembic can still run (produces empty migrations)
    target_metadata = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Read the database URL from LOTAD settings
# ---------------------------------------------------------------------------
from lotad.config import get_settings  # noqa: E402

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override sqlalchemy.url with the value from our settings
config.set_main_option("sqlalchemy.url", get_settings().database_url)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (generates SQL without a live connection)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live database connection."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
