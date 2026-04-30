# LOTAD — Developer Guide

LOTAD (Local Ordered Touhou Arrangements Database) is a CLI tool for ingesting, enriching, and reviewing a personal database of Touhou music arrangements sourced from YouTube playlists and TouhouDB.

**Stack:** Python 3.12 · SQLAlchemy Core + Alembic · Pydantic v2 · Click + Rich · Anthropic SDK · httpx + tenacity + hishel · PostgreSQL via Supabase

---

## Development commands

```sh
uv run lotad                   # run the CLI
uv run pytest                  # run tests
uv run ruff check . --fix      # lint + auto-fix
uv run ruff format .           # format
uv run mypy lotad              # type check
uv run alembic upgrade head    # apply migrations
```

---

## Architecture

| Package | Responsibility |
|---------|---------------|
| `lotad/cli/` | Click CLI surface; each sub-command group has its own file or package |
| `lotad/cli/tasks/` | Task management: list, show, dismiss, resolve, enrich wizards |
| `lotad/ingestion/` | HTTP pipeline: TouhouDB client, YouTube client, song mapper, ingest orchestrator |
| `lotad/agents/` | LLM extraction (Anthropic SDK): video classification + TouhouDB candidate scoring |
| `lotad/tasks/` | Task manager: DB queries and mutations for the human-review queue |
| `lotad/db/` | SQLAlchemy Core table definitions + Alembic migrations |
| `alembic/versions/` | Sequential migration files, prefixed `NNNN_` |

### CLI command packages

Large command groups live in a sub-package (e.g. `lotad/cli/tasks/`). The pattern:

```
tasks/
├── __init__.py   # exports `tasks` group; imports cmd modules to register them
├── _group.py     # defines the Click group — no other imports (avoids circular)
├── _shared.py    # shared helpers and constants used by all cmd modules
├── _actions.py   # ingest actions and interactive editors (called by wizards)
├── _wizards.py   # resolve wizards per task type
├── list_.py      # tasks list command
├── show.py       # tasks show command
├── dismiss.py    # dismiss / bulk-dismiss commands
├── resolve.py    # resolve command (dispatches to wizards)
└── enrich.py     # enrich command (LLM batch enrichment)
```

---

## Code style

### Imports

- **Order:** stdlib → third-party → local (`from lotad.*`), with a blank line between each group.
- **`from __future__ import annotations`** on every non-empty `.py` file.
- **All imports at module level.** Function-level imports are only allowed to break a circular dependency and must be annotated with a comment:
  ```python
  # circular: lotad.cli.originals ↔ lotad.cli.tasks._wizards
  from lotad.cli.originals import _resolve_original_song_chain_tasks
  ```
- Never repeat the same import across 3+ sibling functions — hoist it to module level.

### Type annotations

- All public functions: full annotations (params + return type).
- Private helpers: annotate params; return type required unless `None`.
- **Bare `dict` and `list` are forbidden** — use `dict[str, Any]`, `list[str]`, etc.
- `Any` is acceptable at SQLAlchemy row boundaries and Click option callbacks.

### Naming

| Kind | Convention | Example |
|------|-----------|---------|
| Module-private constant | `_UPPER_SNAKE` | `_CONFIDENCE_COLOR` |
| Module-private function | `_lower_snake` | `_get_data` |
| Public exported constant | `UPPER_SNAKE` | `ENRICH_FAIL_LIMIT` |
| Public function | `verb_noun` | `map_song_to_db` |
| Class | `PascalCase` | `IngestPipeline` |

### Error handling

- **Never swallow exceptions silently.** Always re-raise, log, or return a typed sentinel.
- Use specific exception types (`httpx.HTTPStatusError`, `click.BadParameter`).
- Broad `except Exception` is only acceptable at CLI top-level entry points.
- `raise SomeError(...) from None` is fine for user-facing errors where the chain adds noise.

### Comments

- Write comments to explain **WHY**, never WHAT. Well-named identifiers document what.
- Section banners (`# ---`) only for major logical groupings within a large file — not for individual helpers or fields.
- Magic thresholds and scoring weights **must** have a one-line comment stating their rationale.
- No boilerplate docstrings. One short imperative sentence is always enough:
  ```python
  def get_settings() -> Settings:
      """Return the cached application settings."""
  ```

### Strings

- Always f-strings — never `.format()` or `%`.
- Use `!r` for diagnostic output: `f"Unknown value: {val!r}"`.
- Unicode literals preferred over escape sequences (`…` not `\u2026`).

### Pydantic

- **Always use v2 style:** `model_config = ConfigDict(...)` / `SettingsConfigDict(...)`.
- Never `class Config` (Pydantic v1 style).

### SQLAlchemy

- **Core only** (no ORM). Table objects live in `lotad/db/models.py`.
- Upserts: `pg_insert(...).on_conflict_do_update(...)` — never check-then-insert.
- Queries: `sa.select(table).where(...).limit(...).offset(...)`, executed via `.mappings().all()`.

### Async

- Async functions use `async def` + `await`. Sync CLI entry points call `asyncio.run()`.
- Never mix sync and async logic in the same function body.

### CLI commands

- Each command file registers itself via `@tasks.command(...)` decorating a top-level function.
- For multi-file groups, import the Click group from `_group.py` (not `__init__.py`) to avoid circular imports.
- Prefer `type=click.Choice([...])` for option validation over manual `try/except` + `raise Abort()`.

---

## Anti-patterns — never do these

- `class Config` (Pydantic v1)
- `from module import *`
- Bare `dict` or `list` as type annotations
- Function-level imports without a `# circular:` justification comment
- `except Exception` that swallows without logging or re-raising
- Magic numbers or thresholds without a rationale comment
- Section divider banners (`# ---`) for every function or field

---

## Testing

- **Unit tests** mock external I/O: httpx via `respx`, Anthropic via `unittest.mock`.
- **Integration tests** that hit a real DB or API must be skipped in CI with an env-based condition.
- Test naming: `test_<function>_<scenario>` (e.g. `test_is_album_video_crossfade_title`).
- Run `uv run pytest` before opening a PR.
