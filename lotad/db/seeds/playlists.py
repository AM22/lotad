"""Seed the playlists and scoring_configurations tables.

Run via: lotad db seed-playlists
Or directly: python -m lotad.db.seeds.playlists

Playlist hierarchy (descending quality / weight):
  1. TOUHOU MEGAMIX — permanent favorites
  2. pq             — pending queue; strong candidates for MEGAMIX
  3. REVAL          — songs under revaluation (lower tier)
  4. playlist 3     — saved songs (~5/10); evaluated and liked but below REVAL bar
  5. eval           — unlistened evaluation queue (not yet scored)
"""

from __future__ import annotations

import sys

from sqlalchemy import Engine, insert, select

from lotad.db.models import playlists, scoring_configurations
from lotad.db.session import get_engine

# ---------------------------------------------------------------------------
# Playlist definitions
# Update the youtube_playlist_id values with your real playlist IDs.
# ---------------------------------------------------------------------------

PLAYLISTS = [
    {
        "name": "TOUHOU MEGAMIX",
        "youtube_playlist_id": "PLuDYUKEqeoaxodcKdDwsnjUBt1ZMsOa8q",
        "display_order": 1,
    },
    {
        "name": "pq",
        "youtube_playlist_id": "PLuDYUKEqeoaxLtr2TpS66L-LOvRhiYNnt",
        "display_order": 2,
    },
    {
        "name": "REVAL",
        "youtube_playlist_id": "PLuDYUKEqeoay6kSIsCqY7KA58w--VNX7R",
        "display_order": 3,
    },
    {
        # Saved songs (~5/10); evaluated and liked, below REVAL bar.
        "name": "playlist 3",
        "youtube_playlist_id": "PLuDYUKEqeoazw-dcPeADU3rP0mfZ6nd3V",
        "display_order": 4,
    },
    {
        # Unlistened evaluation queue — songs not yet scored.
        "name": "eval",
        "youtube_playlist_id": "PLuDYUKEqeoawK20u7vX3c6HTnn3w-9oXw",
        "display_order": 5,
    },
]

# ---------------------------------------------------------------------------
# Scoring configurations
#
# Semantic distinction between configs:
#
# "default" — additive / frequency model. Each playlist contributes a number
#   of "points" when aggregating across a body of work. A song that appears in
#   MEGAMIX and REVAL accumulates points from both. Use this config when you
#   care about "how strongly is this original song represented in my collection
#   overall?" Weights are not on a fixed scale.
#     TOUHOU MEGAMIX  = 10 (highest)
#     pq              = 7
#     REVAL           = 4
#     playlist 3      = 1 (evaluated and liked, ~5/10)
#     eval            = 0 (unlistened — must not influence scores)
#
# "ten_point" — rating model. Treat the playlist tier as a score out of 10.
#   Use this config when you want a single number representing "how much do I
#   like this song?" rather than a frequency aggregate. Semantically: a song
#   in MEGAMIX is a 10/10, pq is an 8.5, etc.
#     TOUHOU MEGAMIX  = 10
#     pq              = 8.5
#     REVAL           = 7
#     playlist 3      = 5
#     eval            = 0 (unlistened — no score assigned)
#
# "equal" — all evaluated playlists weighted the same. Useful for raw
#   frequency analysis where tier doesn't matter, only presence.
# ---------------------------------------------------------------------------

SCORING_CONFIGURATIONS = [
    {
        "name": "default",
        "description": (
            "Additive/frequency model. Each playlist tier adds points; eval (unlistened) scores 0."
        ),
        "weights": {
            "TOUHOU MEGAMIX": 10,
            "pq": 7,
            "REVAL": 4,
            "playlist 3": 1,
            "eval": 0,
        },
        "is_default": True,
    },
    {
        "name": "ten_point",
        "description": (
            "Rating model. Playlist tier = score out of 10. "
            "Use for per-song scoring rather than frequency aggregation."
        ),
        "weights": {
            "TOUHOU MEGAMIX": 10,
            "pq": 8.5,
            "REVAL": 7,
            "playlist 3": 5,
            "eval": 0,
        },
        "is_default": False,
    },
    {
        "name": "equal",
        "description": ("All evaluated playlists weighted equally. Use for raw frequency stats."),
        "weights": {
            "TOUHOU MEGAMIX": 1,
            "pq": 1,
            "REVAL": 1,
            "playlist 3": 1,
            "eval": 0,
        },
        "is_default": False,
    },
]


def seed(engine: Engine | None = None) -> None:
    if engine is None:
        engine = get_engine()

    with engine.begin() as conn:
        # Upsert playlists (idempotent — safe to re-run)
        for row in PLAYLISTS:
            exists = conn.execute(
                select(playlists.c.id).where(
                    playlists.c.youtube_playlist_id == row["youtube_playlist_id"]
                )
            ).first()
            if not exists:
                conn.execute(insert(playlists).values(**row))

        # Upsert scoring configurations
        for row in SCORING_CONFIGURATIONS:
            exists = conn.execute(
                select(scoring_configurations.c.id).where(
                    scoring_configurations.c.name == row["name"]
                )
            ).first()
            if not exists:
                conn.execute(insert(scoring_configurations).values(**row))

    print("Seeded playlists and scoring_configurations.")


if __name__ == "__main__":
    seed()
    sys.exit(0)
