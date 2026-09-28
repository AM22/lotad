# Playlist sync and metadata refresh: local testing

## Setup

Use a disposable PostgreSQL copy with the playlist registrations, original-song
catalog, and song associations needed for the scenarios. Set `DATABASE_URL` to
that copy in the shell before running any migration or CLI command; otherwise
the application uses the configured environment or .env database.

For live checks, configure the YouTube and Anthropic API keys as usual. Use
playlists you can edit and a video you control for the private/public transition.
The CLI reads YouTube; playlist edits below are manual actions in YouTube.

~~~sh
uv sync --extra dev
uv run alembic upgrade head
uv run alembic current
~~~

This branch's migration head is **0015**:

- 0013 creates the synthetic unsaved playlist and its scoring weights.
- 0014 records which original associations survive upstream refresh.
- 0015 allows independent dropped-video tasks for each video and source playlist.

Find unsaved by its name/sentinel or `display_order=6`; its database ID is allocated
by the database and need not be 6. The silent-drop policy uses display orders 4
and 5, corresponding to playlist 3 and eval in the standard seed data.

To check rollback, use a separate migration-test copy before the sync scenarios:

~~~sh
uv run alembic downgrade 0012
uv run alembic upgrade head
~~~

Downgrading 0015 refuses to proceed if open tasks would violate the older global
video/song uniqueness rules. Downgrading 0013 requires no associations referencing
unsaved. Use a fresh copy for the migration cycle instead of deleting review
history or song associations to satisfy those conditions.

## Automated checks without live services

~~~sh
CI=true uv run pytest -q
uv run ruff check lotad tests
uv run ruff format --check lotad tests
uv run mypy --follow-imports=silent lotad/sync lotad/cli/sync.py lotad/cli/tasks/_wizards.py lotad/db/models.py lotad/ingestion/mappers.py lotad/ingestion/touhoudb_client.py lotad/ingestion/pipeline.py
~~~

`CI=true` skips the seven live-service ingestion tests. The regression tests use
isolated SQLite databases and mocked external I/O. These checks do not replace
the PostgreSQL migration and live CLI checks below. The mypy command checks the
changed modules and suppresses diagnostics in imported modules.

## Playlist checks

Start with an already-reconciled database snapshot. Disable global stub retry
while testing playlist changes so unrelated stub promotions do not affect the
results:

~~~sh
uv run lotad sync playlist eval --no-retry-stubs
uv run lotad tasks list --type DROPPED_VIDEO
~~~

For cross-playlist moves, include both playlists in one run. Use
`uv run lotad sync all --no-retry-stubs` when every registered playlist belongs
in the test. To restrict the run to two registered YouTube playlist IDs:

~~~python
import asyncio

from lotad.sync.playlist_sync import sync_playlists

report = asyncio.run(
    sync_playlists(["SOURCE_YOUTUBE_PLAYLIST_ID", "TARGET_YOUTUBE_PLAYLIST_ID"],
                   retry_stubs=False)
)
print(report)
~~~

Inspect both the CLI report and the database. The CLI does not expose a single
"removed" counter, and a zero shell exit status alone does not establish that all
per-item operations succeeded.

| Scenario | Expected result |
|---|---|
| Repeat an unchanged, reconciled playlist | Added, Unmatched, move, drop, and error counts stay zero. Checked videos get a new last_checked_at; unchanged metadata retains updated_at. Existing review tasks may remain open. |
| Add a video with a working TouhouDB match | Added increases and an active playlist association appears. |
| Add an unmatched video | Unmatched increases, Added stays zero, and an ingestion-review task is created. |
| Move the same video between two playlists in one run | Moved out/in increase; associations point to the destination, added_at is preserved, and no transient duplicate-song task is needed. |
| Replace an upload with another upload of the same song | The replacement remains active in the intended playlist; removal of the old upload cannot send it to unsaved. |
| Swap two uploads of the same song between playlists | Both destination associations survive with the correct uploads. |
| Remove a previously available video from playlist 3 / eval, with no surviving song association or move | Silent → unsaved increases; matching associations move to unsaved. |
| Remove such a video from MEGAMIX / pq / REVAL | A task with reason=removed_from_playlist identifies the source playlist and associations. |
| Make a listed video private, leaving its playlist entry present | Newly unavailable increases; is_available becomes false; its known title/description survive; a dropped-video task appears even in eval / playlist 3. |
| Repeat the same unavailable response | No duplicate open task is created. An explicitly ignored unchanged incident stays dismissed. |
| Restore that video to public while it remains listed | is_available becomes true and the source playlist's active dropped-video task resolves. |
| Remove an entry already recorded as unavailable | Its associations are soft-deleted and active dropped-video tasks for that source playlist resolve. It is not sent to unsaved. |
| Drop a composite video | All matching track associations are handled; source types and per-track timestamps survive moves. |
| Run with --limit 1 on a playlist with more entries | Unseen associations remain untouched; removals and cross-playlist move detection are disabled. The limit is not a dry run. |

These decisions use playlist membership and last recorded availability. If an
upload becomes private/deleted and disappears from the API response between
polls, the missing-entry path cannot infer its new availability; sync does not
separately probe absent video IDs.

Exercise the resolution wizard on separate tasks:

~~~sh
uv run lotad tasks resolve TASK_ID
~~~

- **U:** move matching associations to unsaved and resolve.
- **D:** soft-delete matching associations and dismiss.
- **I:** dismiss without changing associations.

For U/D, also check a stale task after its association has been replaced: the
replacement stays intact and the stale task remains open.

## Metadata refresh and stub promotion

Prepare a CSV with a `song_id` column containing internal LOTAD IDs, then run:

~~~sh
uv run lotad sync refresh-metadata --csv /path/to/song-ids.csv --dry-run
uv run lotad sync refresh-metadata --csv /path/to/song-ids.csv
~~~

Dry run validates/selects IDs and reports their buckets; it does not fetch remote
metadata or preview field-level differences. A successful refresh count means
the selected record was processed, not necessarily changed.

| Scenario | Expected result |
|---|---|
| Repeat a successful refresh with unchanged upstream data | Song metadata and relationships match; songs.updated_at does not advance. |
| Correct a deliberately altered title/credit in the test database by refreshing | Upstream values are restored and updated_at advances. |
| Replace stale upstream original A with a fully resolved/catalogued B | A is removed, B is linked, and any is_manual=true links survive. |
| Clear the upstream original version | Automatic original links are removed; manual links survive. |
| Resolve an original absent from the local catalog | Existing links survive, known links may be added, and a missing-original task identifies the absent IDs. |
| Encounter a fetch failure, cycle, or depth limit | That song's transaction rolls back and Errors increases; other selected songs can still complete. |
| Retry an eligible stub whose video now matches TouhouDB | Its dependencies and task context transfer to the matched song; the stub is deleted atomically. |
| Retry a stub with no match | It stays intact. |

Use `uv run lotad sync refresh-metadata --song-id STUB_SONG_ID` for one stub;
`--filter stub-retry` selects all eligible non-Original stubs. Default playlist
sync also attempts retries across the database, so use `--no-retry-stubs` when
isolating membership tests.

The HTTP-failure, cycle, partial-catalog, manual-link, and promotion-collision
cases have deterministic coverage in `tests/test_metadata_refresh.py` and
`tests/test_stub_retry.py`. Use those fixtures instead of changing shared
TouhouDB data to manufacture test cases.

Finally, check the existing INGEST_FAILED wizard with a selected test task and
a known TouhouDB ID. Confirm that ingestion, playlist association, and task
resolution still work together.

## Scope

These tests do not establish a complete original list for the reported Eastern
Story medleys. Reading all relevant medley notes and expanding the original
catalog remain separate work.
