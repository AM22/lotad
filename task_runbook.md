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
