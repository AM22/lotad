"""Touhou Wiki slug map for the games we enrich via ``List_by_Song`` pages.

Hard-coded rather than stored as a ``works.wiki_slug`` column — the list is
small and stable, and avoiding a migration keeps the enrichment self-contained.

Scope (matches the M5 enrichment plan):

* PC-98 main games — SoEW (TH02), LLS (TH04), MS (TH05).  HRtP (TH01) is
  excluded because its boss-rush layout doesn't follow the standard
  ``Stage N boss - X's theme`` heading pattern.  PoDD (TH03) is excluded
  because it's a versus game with per-character themes.
* Windows main games — TH06 EoSD through TH20 Fossilized Wonders, excluding
  the PoFV-style versus games (PoFV, UDoALG) that don't fit the stage-based
  heading pattern.
* Photography / spell-practice spin-offs (StB, DS, ISC, VD, 100thBM) and
  fighting games (IaMP, SWR, Hisoutensoku, HM, ULiL, AoCF, GoI) are excluded
  because their music structure doesn't map onto stage / boss themes.  These
  are flagged in the M5 plan as future work.
* Seihou (Shuusou Gyoku, Kioh Gyoku) — included; ZUN composed the soundtracks
  and the wiki ``List_by_Song`` pages exist for them.

``final_stage_number`` is used to resolve ``Final boss - X's theme`` headings
to a concrete stage integer (since the heading itself doesn't say which
numbered stage is the final one).  All Windows-era main games are 6 stages.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WikiGame:
    """A game we enrich via Touhou Wiki.

    ``works_short_name`` matches ``works.short_name`` in the DB; we look up the
    work id at runtime via that column rather than carrying numeric ids here
    (which would couple this module to a particular DB instance).
    """

    works_short_name: str
    wiki_slug: str
    final_stage_number: int
    has_spell_cards: bool


# Order roughly mirrors release chronology so dry-run output reads nicely.
GAMES: tuple[WikiGame, ...] = (
    # PC-98 era — no spell cards (the system was introduced in TH06)
    WikiGame("SoEW", "Story_of_Eastern_Wonderland", final_stage_number=5, has_spell_cards=False),
    WikiGame("LLS", "Lotus_Land_Story", final_stage_number=6, has_spell_cards=False),
    WikiGame("MS", "Mystic_Square", final_stage_number=6, has_spell_cards=False),
    # Windows main series
    WikiGame("EoSD", "Embodiment_of_Scarlet_Devil", final_stage_number=6, has_spell_cards=True),
    WikiGame("PCB", "Perfect_Cherry_Blossom", final_stage_number=6, has_spell_cards=True),
    WikiGame("IN", "Imperishable_Night", final_stage_number=6, has_spell_cards=True),
    WikiGame("MoF", "Mountain_of_Faith", final_stage_number=6, has_spell_cards=True),
    WikiGame("SA", "Subterranean_Animism", final_stage_number=6, has_spell_cards=True),
    WikiGame("UFO", "Undefined_Fantastic_Object", final_stage_number=6, has_spell_cards=True),
    WikiGame("TD", "Ten_Desires", final_stage_number=6, has_spell_cards=True),
    WikiGame("DDC", "Double_Dealing_Character", final_stage_number=6, has_spell_cards=True),
    WikiGame("LoLK", "Legacy_of_Lunatic_Kingdom", final_stage_number=6, has_spell_cards=True),
    WikiGame("HSiFS", "Hidden_Star_in_Four_Seasons", final_stage_number=6, has_spell_cards=True),
    WikiGame(
        "WBaWC", "Wily_Beast_and_Weakest_Creature", final_stage_number=6, has_spell_cards=True
    ),
    WikiGame("UM", "Unconnected_Marketeers", final_stage_number=6, has_spell_cards=True),
    WikiGame("FW", "Fossilized_Wonders", final_stage_number=6, has_spell_cards=True),
    # Seihou — ZUN composed the soundtrack
    WikiGame("SG", "Shuusou_Gyoku", final_stage_number=5, has_spell_cards=False),
    WikiGame("KG", "Kioh_Gyoku", final_stage_number=5, has_spell_cards=False),
)


GAMES_BY_SLUG: dict[str, WikiGame] = {g.wiki_slug: g for g in GAMES}
GAMES_BY_SHORT_NAME: dict[str, WikiGame] = {g.works_short_name: g for g in GAMES}
