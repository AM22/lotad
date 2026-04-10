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
        # Disable psycopg3 server-side prepared statements.
        #
        # psycopg3 automatically prepares frequently-executed statements
        # server-side (named _pg3_0, _pg3_1, …).  When a transaction is
        # rolled back after an error, psycopg3's internal cache of "what is
        # already prepared on this connection" can drift from the server's
        # actual state.  On the next pool reuse of that connection, psycopg3
        # tries to PREPARE the same statement again and gets
        # DuplicatePreparedStatement.  Setting prepare_threshold=None
        # disables this promotion entirely; psycopg3 uses the extended query
        # protocol instead, which is still efficient and avoids the bug.
        connect_args={"prepare_threshold": None},
    )


def get_session_factory():
    engine = get_engine()
    return sessionmaker(bind=engine, autocommit=False, autoflush=False)
