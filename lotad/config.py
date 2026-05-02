"""Central configuration loaded from environment variables / .env file."""

from __future__ import annotations

from functools import lru_cache

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: str = Field(..., description="PostgreSQL DSN, e.g. postgresql+psycopg://...")

    anthropic_api_key: str = Field(..., description="Anthropic API key for Claude")
    anthropic_model: str = Field(
        "claude-sonnet-4-6", description="Model ID for metadata extraction"
    )

    youtube_api_key: str = Field(
        ..., description="YouTube Data API v3 key (read-only, public/unlisted playlists)"
    )

    touhoudb_base_url: str = Field(
        "https://touhoudb.com/api", description="TouhouDB REST API base URL"
    )
    touhoudb_request_timeout: float = Field(
        10.0,
        description=(
            "Per-request timeout in seconds. TouhouDB should respond in <3s under normal load; "
            "10s gives a comfortable margin while keeping failure detection fast."
        ),
    )
    touhoudb_max_retries: int = Field(
        3,
        description="Max retry attempts per TouhouDB request (tenacity exponential back-off).",
    )
    touhoudb_circuit_breaker_threshold: int = Field(
        10, description="Consecutive failures before circuit opens"
    )
    touhoudb_username: str | None = Field(
        None,
        description=(
            "TouhouDB username for the contribution (write) flow. The password is "
            "prompted interactively at `lotad contrib login` and never persisted; "
            "only the resulting session cookie is stored to disk."
        ),
    )
    touhoudb_session_path: str = Field(
        ".cache/touhoudb_session.json",
        description="Path where the authenticated TouhouDB session is persisted between commands.",
    )
    touhoudb_edit_user_agent: str = Field(
        "LOTAD-contrib/0.1 (+https://github.com/AM22/lotad)",
        description="User-Agent for write traffic; distinguished from read-side ingestion.",
    )

    normalization_ttl_hours: int = Field(
        24, description="Hours before normalization metrics are considered stale"
    )

    ingestion_checkpoint_path: str = Field(
        ".lotad_checkpoint.json",
        description="Path to resume token file for interrupted ingestion runs",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
