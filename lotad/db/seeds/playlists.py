"""Seed the playlists and scoring_configurations tables.

Run via: lotad db seed-playlists
Or directly: python -m lotad.db.seeds.playlists

YouTube playlist IDs are placeholders — update them with your real IDs before
running against a production database. The IDs below are clearly fake so they
will fail loudly if used against the YouTube API without replacement.
"""

from __future__ import annotations

import sys

from sqlalchemy import insert, select

from lotad.db.models import playlists, scoring_configurations
from lotad.db.session import get_engine

# ---------------------------------------------------------------------------
# Playlist definitions
# Update the youtube_playlist_id values with your real playlist IDs.
# ---------------------------------------------------------------------------

PLAYLISTS = [
    {
        "name": "TOUHOU MEGAMIX",
        "youtube_playlist_id": "PLACEHOLDER_MEGAMIX",
        "display_order": 1,
    },
    {
        "name": "pq",
        "youtube_playlist_id": "PLACEHOLDER_PQ",
        "display_order": 2,
    },
    {
        "name": "REVAL",
        "youtube_playlist_id": "PLACEHOLDER_REVAL",
        "display_order": 3,
    },
    {
        "name": "eval",
        "youtube_playlist_id": "PLACEHOLDER_EVAL",
        "display_order": 4,
    },
]

# ---------------------------------------------------------------------------
# Scoring configurations
#
# "default" — intended for primary analytics. Weights reflect playlist tiers:
#   TOUHOU MEGAMIX = permanent favourites (highest weight)
#   pq = pending queue / strong candidates
#   REVAL = revaluation (lower tier, to be re-evaluated)
#   eval = evaluation (unscored, weight 0 so they don't skew results)
#
# "equal" — all playlists weighted the same; useful for raw frequency analysis.
# ---------------------------------------------------------------------------

SCORING_CONFIGURATIONS = [
    {
        "name": "default",
        "description": (
            "Primary config. MEGAMIX songs count most, eval songs are unscored."
        ),
        "weights": {
            "TOUHOU MEGAMIX": 10,
            "pq": 7,
            "REVAL": 4,
            "eval": 0,
        },
        "is_default": True,
    },
    {
        "name": "equal",
        "description": "All playlists weighted equally. Use for raw frequency stats.",
        "weights": {
            "TOUHOU MEGAMIX": 1,
            "pq": 1,
            "REVAL": 1,
            "eval": 1,
        },
        "is_default": False,
    },
]


def seed(engine=None) -> None:
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
