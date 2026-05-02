# Task Runbook

SQL queries and investigation steps for each task type. Run these against the
Postgres database to gather context before resolving a task.

---

## DROPPED_VIDEO

A video in a YouTube playlist returned a "Deleted video" or "Private video" stub.
The content is gone but the slot still exists in the playlist. The goal is to
identify what the song probably was so it can be manually added to the DB.

### Task `data` fields

| Field | Description |
|---|---|
| `video_id` | YouTube 11-character video ID |
| `title` | Stub title (`"Deleted video"` or `"Private video"`) |
| `position` | 0-based index of this video in the YouTube playlist |
| `playlist_db_id` | Internal `playlists.id` the video belongs to (nullable if ingested outside a playlist) |
| `note` | Static explanation string |

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

Once the song is identified:
1. Add it to TouhouDB if missing.
2. Manually insert a row into `songs` and link it.
3. Mark the task `RESOLVED` with a note on what the video was.
4. If the video is truly unidentifiable, mark it `DISMISSED`.

---

## Bulk metadata refresh queries

`lotad sync refresh-metadata --csv path.csv` accepts a CSV with one `song_id`
column. Generate the input by running these queries in Supabase and using
"Export as CSV":

### Eastern Story chain backfill (one-time, M5 leftover)

Songs ingested before the テーマ・オブ・イースタンストーリー exception in
`resolve_original_chain` was added.  Their `song_originals` rows link only to
`original_songs.touhoudb_id = 2445` and are missing the intermediate ZUN
parent (Necrofantasia, etc.).

```sql
SELECT s.id AS song_id
FROM songs s
JOIN song_originals so ON so.song_id = s.id
JOIN original_songs o   ON o.id = so.original_song_id
WHERE o.touhoudb_id = 2445
  AND s.touhoudb_id IS NOT NULL
GROUP BY s.id
HAVING COUNT(*) = 1;
```

Re-run the metadata refresh after `lotad originals scrape` (so the
intermediate originals exist in `original_songs`).

### Other one-off pulls

Use the same pattern: write a query that returns `song_id`, export CSV, run
`lotad sync refresh-metadata --csv path.csv`.  Or use the built-in filter
presets:

- `--filter missing-lyricist` — songs with `has_lyrics=true` and no LYRICIST credit
- `--filter zero-duration`    — songs with NULL or 0 `duration_seconds`
- `--filter stub-retry`       — stub songs (no `touhoudb_id`) with non-ORIGINAL `song_type`
