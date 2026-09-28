# Task Runbook

SQL queries and investigation steps for each task type. Run these against the
Postgres database to gather context before resolving a task.

For local validation, see [playlist sync and metadata refresh tests](docs/testing-sync.md).

---

## DROPPED_VIDEO

A dropped-video task can represent an unavailable upload that remains in a
playlist, or an entry removed from a higher-rated playlist. Read the reason
and source playlist before choosing an action.

| Observation | Sync behavior |
|---|---|
| Deleted/private placeholder still listed, in any playlist | Preserve known metadata, mark unavailable, create or update a review task |
| Entry absent from playlist 3 / eval, previously available, with no move or surviving song association | Move to unsaved |
| Same absence from MEGAMIX / pq / REVAL | Create a removal-review task |
| Previously unavailable entry disappears | Soft-delete the association and resolve its dropped-video task |

Absence is determined from a complete playlist response. If an upload becomes
unavailable and disappears between polls, sync only has its last recorded
availability; it does not separately probe missing video IDs. Limited syncs
skip removal handling.

### Task `data` fields

| Field | Description |
|---|---|
| `video_id` | YouTube 11-character video ID |
| `title` | Last known title when available; otherwise the deleted/private placeholder |
| `position` | 0-based index of this video in the YouTube playlist |
| `playlist_db_id` | Internal `playlists.id` the video belongs to (nullable if ingested outside a playlist) |
| `reason` | `deleted` or `removed_from_playlist` for sync-created tasks; may be absent on ingestion tasks |
| `note` | Explanation or resolution context, when present |
| `playlist_song_id` / `playlist_song_ids` | Association(s) affected by sync, including every track of composite videos |
| `source_playlist_db_id` | Source playlist for validating removal actions |

### Step 1 — Get playlist info

```sql
SELECT id, name, youtube_playlist_id
FROM playlists
WHERE id = <playlist_db_id>;
```

Use `youtube_playlist_id` to open the playlist on YouTube:
`https://www.youtube.com/playlist?list=<youtube_playlist_id>`

Navigate to position `<position + 1>` (1-based on YouTube) to see what is
directly before and after the gap.

### Step 2 — Channel distribution in the playlist

See which circles/channels appear most around the dropped video's position,
to estimate the dropped video's origin.

```sql
SELECT
    yv.channel_name,
    COUNT(*) AS track_count
FROM playlist_songs ps
JOIN youtube_videos yv ON yv.id = ps.youtube_video_id
WHERE
    ps.playlist_id = <playlist_db_id>
    AND ps.removed_at IS NULL
    AND yv.is_available = TRUE
GROUP BY yv.channel_name
ORDER BY track_count DESC;
```

### Step 3 — Songs near the dropped position (by DB insertion order)

`playlist_songs` does not store the original YouTube playlist position.
`added_at` approximates insertion order within a single ingest run, which
correlates with position order.

```sql
SELECT
    yv.video_id,
    yv.title,
    yv.channel_name,
    ps.added_at
FROM playlist_songs ps
JOIN youtube_videos yv ON yv.id = ps.youtube_video_id
WHERE
    ps.playlist_id = <playlist_db_id>
    AND ps.removed_at IS NULL
ORDER BY ps.added_at
LIMIT 20 OFFSET GREATEST(0, <position> - 10);
```

> **Note:** `OFFSET <position> - 10` is an approximation. For a more accurate
> neighbourhood, open the YouTube playlist directly (Step 1) and look at the
> surrounding entries.

### Step 4 — Resolution

Run `uv run lotad tasks resolve <task_id>`:

- **U** moves the still-matching associations to unsaved and resolves the task.
- **D** soft-deletes those associations and dismisses the task.
- **I** dismisses the task without changing playlist associations.

U and D validate the source playlist, video, and song identities before writing.
If the association was replaced or no longer exists, they leave the task open.
A composite video's task covers all of its matching track associations.

---

## Bulk metadata refresh queries

`lotad sync refresh-metadata --csv path.csv` accepts a CSV with one `song_id`
column. Generate the input by running these queries in Supabase and using
"Export as CSV":

### Eastern Story review candidates

This query finds songs whose only original link is テーマ・オブ・イースタンストーリー.
They are candidates for review, not proof of an incorrect mapping.

~~~sql
SELECT s.id AS song_id
FROM songs s
JOIN song_originals so ON so.song_id = s.id
JOIN original_songs o ON o.id = so.original_song_id
WHERE s.touhoudb_id IS NOT NULL
GROUP BY s.id
HAVING COUNT(*) = 1 AND BOOL_AND(o.touhoudb_id = 2445);
~~~

Refresh uses the existing original-chain resolver. It still reads extra source
references only from the penultimate node, so it does not yet recover all sources
listed on a medley's own entry. The catalog also excludes some official themes
typed Arrangement and some non-ZUN composers; rerunning the existing scraper
does not fill those gaps. Resolver and catalog improvements are separate work.

### Refresh semantics and migrations

Run Alembic through revision 0015 before using this branch. A complete successful
resolution replaces upstream original links, including clearing them when
TouhouDB removes the original version. Manual links marked
song_originals.is_manual = TRUE survive refresh, as do links extracted for stubs
and retained during their promotion. The migration marks existing stub links
as manual; other existing links are treated as upstream, matching the current
application's write paths. If you inserted overrides directly with SQL on linked
songs, mark those links is_manual = TRUE before refreshing.

When some resolved IDs are absent from the original catalog, refresh adds the
known links, preserves existing links, and raises a FILL_MISSING_INFO task for
the missing IDs. Add the missing originals and refresh that song again to replace
the old upstream set. Failed HTTP requests, cycles, or depth-limited resolution
roll back the song's refresh and increment the error count.

Revision 0015 allows independent dropped-video tasks for the same upload in
different playlists while retaining one open task per upload and playlist.

Playlist sync does not refresh metadata for every existing linked song. An
upstream correction requires an explicit refresh-metadata run.

### Other one-off pulls

Use the same pattern: write a query that returns `song_id`, export CSV, run
`lotad sync refresh-metadata --csv path.csv`.  Or use the built-in filter
presets:

- `--filter missing-lyricist` — songs with `has_lyrics=true` and no LYRICIST credit
- `--filter zero-duration`    — songs with NULL or 0 `duration_seconds`
- `--filter stub-retry`       — stub songs (no `touhoudb_id`) with non-ORIGINAL `song_type`
