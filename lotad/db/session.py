"""SQLAlchemy engine and session factory."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from lotad.config import get_settings


def get_engine():
    """Return a SQLAlchemy engine bound to the configured database URL."""
    settings = get_settings()
    return create_engine(
        settings.database_url,
        pool_pre_ping=True,  # detect stale connections
        echo=False,
    )


def get_session_factory():
    engine = get_engine()
    return sessionmaker(bind=engine, autocommit=False, autoflush=False)
