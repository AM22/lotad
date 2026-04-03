"""Seed the playlists and scoring_configurations tables.

Run via: lotad db seed-playlists
Or directly: python -m lotad.db.seeds.playlists

Playlist hierarchy (descending quality / weight):
  1. TOUHOU MEGAMIX — permanent favourites
  2. pq             — pending queue; strong candidates for MEGAMIX
  3. REVAL          — songs under revaluation (lower tier)
  4. eval           — unscored evaluation pool
  5. playlist 3     — liked but not good enough for any of the above
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
        "name": "eval",
        "youtube_playlist_id": "PLuDYUKEqeoazw-dcPeADU3rP0mfZ6nd3V",
        "display_order": 4,
    },
    {
        # Songs worth keeping but not strong enough for REVAL/eval.
        "name": "playlist 3",
        "youtube_playlist_id": "PLuDYUKEqeoawK20u7vX3c6HTnn3w-9oXw",
        "display_order": 5,
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
#   playlist 3 = saved but not strong; weight 0 for primary analytics
#
# "equal" — all playlists weighted the same; useful for raw frequency analysis.
# ---------------------------------------------------------------------------

SCORING_CONFIGURATIONS = [
    {
        "name": "default",
        "description": (
            "Primary config. MEGAMIX songs count most; eval and playlist 3 are unscored."
        ),
        "weights": {
            "TOUHOU MEGAMIX": 10,
            "pq": 7,
            "REVAL": 4,
            "eval": 0,
            "playlist 3": 0,
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
            "playlist 3": 1,
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
