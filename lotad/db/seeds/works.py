"""Seed the works table with canonical Touhou games, ZUN music CDs, and books.

Also includes Seihou (ZUN Soft) games which ZUN composed music for — these
are not Touhou games but ZUN is credited as composer, so their songs appear
in TouhouDB and arrangements of them can appear in user playlists.

Sources:
  - https://en.touhouwiki.net/wiki/Touhou_Project
  - https://en.touhouwiki.net/wiki/Seihou_Project
  - https://touhoudb.com (verify TouhouDB model alignment during M2)

Run via: lotad db seed-works
Or directly: python -m lotad.db.seeds.works

This seed is idempotent — re-running will not create duplicates.
"""

from __future__ import annotations

import sys
from decimal import Decimal

from sqlalchemy import insert, select

from lotad.db.models import works
from lotad.db.session import get_engine

# ---------------------------------------------------------------------------
# Canonical games (PC-98 era + Windows era)
# canonical_order uses Decimal to allow fractional entries (e.g. 7.5 for IaMP)
# ---------------------------------------------------------------------------

GAMES = [
    # PC-98 era
    {
        "name": "Highly Responsive to Prayers",
        "short_name": "HRtP",
        "media_type": "GAME",
        "release_year": 1997,
        "canonical_order": Decimal("1"),
    },
    {
        "name": "Story of Eastern Wonderland",
        "short_name": "SoEW",
        "media_type": "GAME",
        "release_year": 1997,
        "canonical_order": Decimal("2"),
    },
    {
        "name": "Phantasmagoria of Dim.Dream",
        "short_name": "PoDD",
        "media_type": "GAME",
        "release_year": 1997,
        "canonical_order": Decimal("3"),
    },
    {
        "name": "Lotus Land Story",
        "short_name": "LLS",
        "media_type": "GAME",
        "release_year": 1998,
        "canonical_order": Decimal("4"),
    },
    {
        "name": "Mystic Square",
        "short_name": "MS",
        "media_type": "GAME",
        "release_year": 1998,
        "canonical_order": Decimal("5"),
    },
    # Windows era — main series
    {
        "name": "Embodiment of Scarlet Devil",
        "short_name": "EoSD",
        "media_type": "GAME",
        "release_year": 2002,
        "canonical_order": Decimal("6"),
    },
    {
        "name": "Perfect Cherry Blossom",
        "short_name": "PCB",
        "media_type": "GAME",
        "release_year": 2003,
        "canonical_order": Decimal("7"),
    },
    {
        "name": "Immaterial and Missing Power",
        "short_name": "IaMP",
        "media_type": "GAME",
        "release_year": 2004,
        "canonical_order": Decimal("7.5"),
        "notes": "Fighter; co-developed with Twilight Frontier",
    },
    {
        "name": "Imperishable Night",
        "short_name": "IN",
        "media_type": "GAME",
        "release_year": 2004,
        "canonical_order": Decimal("8"),
    },
    {
        "name": "Phantasmagoria of Flower View",
        "short_name": "PoFV",
        "media_type": "GAME",
        "release_year": 2005,
        "canonical_order": Decimal("9"),
    },
    {
        "name": "Shoot the Bullet",
        "short_name": "StB",
        "media_type": "GAME",
        "release_year": 2005,
        "canonical_order": Decimal("9.5"),
        "notes": "Photography spin-off",
    },
    {
        "name": "Mountain of Faith",
        "short_name": "MoF",
        "media_type": "GAME",
        "release_year": 2007,
        "canonical_order": Decimal("10"),
    },
    {
        "name": "Scarlet Weather Rhapsody",
        "short_name": "SWR",
        "media_type": "GAME",
        "release_year": 2008,
        "canonical_order": Decimal("10.5"),
        "notes": "Fighter; co-developed with Twilight Frontier",
    },
    {
        "name": "Subterranean Animism",
        "short_name": "SA",
        "media_type": "GAME",
        "release_year": 2008,
        "canonical_order": Decimal("11"),
    },
    {
        "name": "Undefined Fantastic Object",
        "short_name": "UFO",
        "media_type": "GAME",
        "release_year": 2009,
        "canonical_order": Decimal("12"),
    },
    {
        "name": "Touhou Hisoutensoku",
        "short_name": "Hisoutensoku",
        "media_type": "GAME",
        "release_year": 2009,
        "canonical_order": Decimal("12.3"),
        "notes": "Fighter expansion; co-developed with Twilight Frontier",
    },
    {
        "name": "Double Spoiler",
        "short_name": "DS",
        "media_type": "GAME",
        "release_year": 2010,
        "canonical_order": Decimal("12.5"),
        "notes": "Photography spin-off",
    },
    {
        "name": "Great Fairy Wars",
        "short_name": "GFW",
        "media_type": "GAME",
        "release_year": 2010,
        "canonical_order": Decimal("12.8"),
        "notes": "Spin-off",
    },
    {
        "name": "Ten Desires",
        "short_name": "TD",
        "media_type": "GAME",
        "release_year": 2011,
        "canonical_order": Decimal("13"),
    },
    {
        "name": "Hopeless Masquerade",
        "short_name": "HM",
        "media_type": "GAME",
        "release_year": 2013,
        "canonical_order": Decimal("13.5"),
        "notes": "Fighter; co-developed with Twilight Frontier",
    },
    {
        "name": "Double Dealing Character",
        "short_name": "DDC",
        "media_type": "GAME",
        "release_year": 2013,
        "canonical_order": Decimal("14"),
    },
    {
        "name": "Impossible Spell Card",
        "short_name": "ISC",
        "media_type": "GAME",
        "release_year": 2014,
        "canonical_order": Decimal("14.3"),
        "notes": "Spin-off",
    },
    {
        "name": "Urban Legend in Limbo",
        "short_name": "ULiL",
        "media_type": "GAME",
        "release_year": 2015,
        "canonical_order": Decimal("14.5"),
        "notes": "Fighter; co-developed with Twilight Frontier",
    },
    {
        "name": "Legacy of Lunatic Kingdom",
        "short_name": "LoLK",
        "media_type": "GAME",
        "release_year": 2015,
        "canonical_order": Decimal("15"),
    },
    {
        "name": "Antinomy of Common Flowers",
        "short_name": "AoCF",
        "media_type": "GAME",
        "release_year": 2017,
        "canonical_order": Decimal("15.5"),
        "notes": "Fighter; co-developed with Twilight Frontier",
    },
    {
        "name": "Hidden Star in Four Seasons",
        "short_name": "HSiFS",
        "media_type": "GAME",
        "release_year": 2017,
        "canonical_order": Decimal("16"),
    },
    {
        "name": "Violet Detector",
        "short_name": "VD",
        "media_type": "GAME",
        "release_year": 2018,
        "canonical_order": Decimal("16.5"),
        "notes": "Photography spin-off",
    },
    {
        "name": "Wily Beast and Weakest Creature",
        "short_name": "WBaWC",
        "media_type": "GAME",
        "release_year": 2019,
        "canonical_order": Decimal("17"),
    },
    {
        "name": "Touhou Gouyoku Ibun",
        "short_name": "GoI",
        "media_type": "GAME",
        "release_year": 2021,
        "canonical_order": Decimal("17.5"),
        "notes": "Fighter; co-developed with Twilight Frontier",
    },
    {
        "name": "Unconnected Marketeers",
        "short_name": "UM",
        "media_type": "GAME",
        "release_year": 2021,
        "canonical_order": Decimal("18"),
    },
    {
        "name": "100th Black Market",
        "short_name": "100thBM",
        "media_type": "GAME",
        "release_year": 2022,
        "canonical_order": Decimal("18.5"),
        "notes": "Spin-off",
    },
    {
        "name": "Unfinished Dream of All Living Ghost",
        "short_name": "UDoALG",
        "media_type": "GAME",
        "release_year": 2023,
        "canonical_order": Decimal("19"),
    },
    {
        "name": "Fossilized Wonders",
        "short_name": "FW",
        "media_type": "GAME",
        "release_year": 2025,
        "canonical_order": Decimal("20"),
    },
]

# ---------------------------------------------------------------------------
# ZUN music CDs (Ghostly Field Club series and others)
# canonical_order is null for music CDs — they are not part of the game sequence
# ---------------------------------------------------------------------------

MUSIC_CDS = [
    {
        "name": "Ghostly Field Club",
        "short_name": "GFC",
        "media_type": "MUSIC_CD",
        "release_year": 2003,
    },
    {
        "name": "Dolls in Pseudo Paradise",
        "short_name": "DiPP",
        "media_type": "MUSIC_CD",
        "release_year": 2002,
    },
    {
        "name": "Changeability of Strange Dream",
        "short_name": "CoSD",
        "media_type": "MUSIC_CD",
        "release_year": 2004,
    },
    {
        "name": "Retrospective 53 minutes",
        "short_name": "R53",
        "media_type": "MUSIC_CD",
        "release_year": 2007,
    },
    {
        "name": "Magical Astronomy",
        "short_name": "MA",
        "media_type": "MUSIC_CD",
        "release_year": 2006,
    },
    {
        "name": "Unknown Flower, Mesmerizing Journey",
        "short_name": "UFMJ",
        "media_type": "MUSIC_CD",
        "release_year": 2008,
    },
    {
        "name": "Trojan Green Asteroid",
        "short_name": "TGA",
        "media_type": "MUSIC_CD",
        "release_year": 2010,
    },
    {
        "name": "Neo-traditionalism of Japan",
        "short_name": "NtJ",
        "media_type": "MUSIC_CD",
        "release_year": 2011,
    },
    {
        "name": "Dr. Latency's Freak Report",
        "short_name": "DLFR",
        "media_type": "MUSIC_CD",
        "release_year": 2013,
    },
    {
        "name": "Dateless Bar Old Adam",
        "short_name": "DBOA",
        "media_type": "MUSIC_CD",
        "release_year": 2016,
    },
    {
        "name": "Rainbow-Colored Septentrion",
        "short_name": "RCS",
        "media_type": "MUSIC_CD",
        "release_year": 2021,
    },
    {
        "name": "Taboo Japan Disentanglement",
        "short_name": "TJD",
        "media_type": "MUSIC_CD",
        "release_year": 2024,
    },
]

# ---------------------------------------------------------------------------
# Print works (manga / books where characters first appear)
# canonical_order is null — these are not part of the game number sequence
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Seihou (ZUN Soft) games — not Touhou, but ZUN composed their soundtracks.
# canonical_order is null — these are outside the Touhou numbering system.
# touhoudb_id stores the TouhouDB album ID for deterministic work matching.
# ---------------------------------------------------------------------------

SEIHOU_GAMES = [
    {
        "name": "Shuusou Gyoku",
        "short_name": "SG",
        "media_type": "GAME",
        "release_year": 2000,
        "touhoudb_id": 28,  # https://touhoudb.com/Al/28
        "notes": "Seihou #1; co-developed with Amusement Makers",
    },
    {
        "name": "Kioh Gyoku",
        "short_name": "KG",
        "media_type": "GAME",
        "release_year": 2001,
        "touhoudb_id": 136,  # https://touhoudb.com/Al/136
        "notes": "Seihou #2; co-developed with Amusement Makers",
    },
]


BOOKS = [
    {
        "name": "Curiosities of Lotus Asia",
        "short_name": "CoLA",
        "media_type": "BOOK",
        "release_year": 2004,
        "notes": "Manga serialization; Rinnosuke introduced",
    },
    {
        "name": "Silent Sinner in Blue",
        "short_name": "SSiB",
        "media_type": "BOOK",
        "release_year": 2007,
        "notes": "Manga; Watatsuki sisters first appear",
    },
    {
        "name": "Cage in Lunatic Runagate",
        "short_name": "CiLR",
        "media_type": "BOOK",
        "release_year": 2009,
        "notes": "Novel",
    },
    {
        "name": "Perfect Memento in Strict Sense",
        "short_name": "PMiSS",
        "media_type": "BOOK",
        "release_year": 2006,
        "notes": "Guidebook; Hieda no Akyuu introduced",
    },
    {
        "name": "The Grimoire of Marisa",
        "short_name": "GoM",
        "media_type": "BOOK",
        "release_year": 2009,
        "notes": "Guidebook",
    },
    {
        "name": "Inaban of the Moon and Inaban of the Earth",
        "short_name": "ItMaItE",
        "media_type": "BOOK",
        "release_year": 2010,
        "notes": "4-koma manga",
    },
    {
        "name": "Forbidden Scrollery",
        "short_name": "FS",
        "media_type": "BOOK",
        "release_year": 2012,
        "notes": "Manga",
    },
    {
        "name": "Symposium of Post-mysticism",
        "short_name": "SoPm",
        "media_type": "BOOK",
        "release_year": 2012,
        "notes": "Guidebook",
    },
    {
        "name": "The Grimoire of Usami",
        "short_name": "GoU",
        "media_type": "BOOK",
        "release_year": 2019,
        "notes": "Guidebook; HSiFS characters",
    },
    {
        "name": "Lotus Eaters",
        "short_name": "LE",
        "media_type": "BOOK",
        "release_year": 2020,
        "notes": "Manga",
    },
    {
        "name": "Wild and Horned Hermit",
        "short_name": "WaHH",
        "media_type": "BOOK",
        "release_year": 2010,
        "notes": "Manga; Kasen introduced",
    },
    {
        "name": "Foul Detective Satori",
        "short_name": "FDS",
        "media_type": "BOOK",
        "release_year": 2019,
        "notes": "Manga (also called 'Cheating Detective Satori'); serialized on ComicWalker",
    },
    {
        "name": "Strange Creators of Outer World",
        "short_name": "SCooW",
        "media_type": "BOOK",
        "release_year": 2015,
        "notes": "Anthology / artbook series",
    },
    # Bunbunmaru newspaper compilation with ZUN-composed theme music
    {
        "name": "Bohemian Archive in Japanese Red",
        "short_name": "BAiJR",
        "media_type": "BOOK",
        "release_year": 2005,
        "notes": "Bunbunmaru Shinbun newspaper anthology; includes original ZUN tracks",
    },
    # Three Fairies manga (Strange and Bright Nature Deity arc) — TouhouDB stores
    # the three tankōbon volumes as separate albums.
    # NOTE: if TouhouDB names these without volume numbers, the difflib matcher
    # will attribute all volumes to the first (2008) entry. Add touhoudb_ids once
    # confirmed to switch to the deterministic fast-path.
    {
        "name": "Strange and Bright Nature Deity",
        "short_name": "SaBND",
        "media_type": "BOOK",
        "release_year": 2008,
        "notes": "Three Fairies manga volume 1",
    },
    {
        "name": "Strange and Bright Nature Deity Volume 2",
        "short_name": "SaBND2",
        "media_type": "BOOK",
        "release_year": 2009,
        "notes": "Three Fairies manga volume 2",
    },
    {
        "name": "Strange and Bright Nature Deity Volume 3",
        "short_name": "SaBND3",
        "media_type": "BOOK",
        "release_year": 2009,
        "notes": "Three Fairies manga volume 3",
    },
]


def seed(engine=None) -> None:
    if engine is None:
        engine = get_engine()

    all_works = GAMES + SEIHOU_GAMES + MUSIC_CDS + BOOKS
    inserted = 0
    updated = 0

    with engine.begin() as conn:
        for row in all_works:
            # Idempotent: skip if already present (match on name + media_type)
            existing = conn.execute(
                select(works.c.id, works.c.touhoudb_id).where(
                    (works.c.name == row["name"]) & (works.c.media_type == row["media_type"])
                )
            ).first()
            if not existing:
                conn.execute(insert(works).values(**row))
                inserted += 1
            elif "touhoudb_id" in row and existing.touhoudb_id is None:
                # Back-fill touhoudb_id if the row exists but lacks it
                conn.execute(
                    works.update()
                    .where(works.c.id == existing.id)
                    .values(touhoudb_id=row["touhoudb_id"])
                )
                updated += 1

    print(
        f"Seeded works: {inserted} new rows, {updated} updated "
        f"({len(GAMES)} Touhou games, {len(SEIHOU_GAMES)} Seihou games, "
        f"{len(MUSIC_CDS)} music CDs, {len(BOOKS)} books)."
    )


if __name__ == "__main__":
    seed()
    sys.exit(0)
