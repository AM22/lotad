"""Unit tests for the Touhou Wiki parser — no network required."""

from __future__ import annotations

import pytest

from lotad.ingestion.touhouwiki_parser import (
    extract_japanese_title,
    name_token_set,
    normalize_japanese_title,
    parse_section_heading,
    parse_spell_card_owners,
    split_sections_by_heading,
)

# ---------------------------------------------------------------------------
# parse_section_heading
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "line, final_stage, expected",
    [
        # Stage themes (no character in heading)
        ("Stage 1 theme", 6, (1, False, None, False)),
        ("Stage 5 theme", 6, (5, False, None, False)),
        # Stage boss themes
        ("Stage 5 boss - Sakuya Izayoi's theme", 6, (5, True, "Sakuya Izayoi", False)),
        ("Stage 3 boss - Hong Meiling's theme", 6, (3, True, "Hong Meiling", False)),
        # Final stage / final boss
        ("Final stage theme", 6, (6, False, None, False)),
        ("Final boss - Remilia Scarlet's theme", 6, (6, True, "Remilia Scarlet", False)),
        ("Final stage boss - Saki Kurokoma's theme", 6, (6, True, "Saki Kurokoma", False)),
        # Extra
        ("Extra stage theme", 6, (7, False, None, False)),
        ("Extra stage boss - Flandre Scarlet's theme", 6, (7, True, "Flandre Scarlet", False)),
        ("Extra boss - Koishi Komeiji's theme", 6, (7, True, "Koishi Komeiji", False)),
        # Phantasm (PCB only) — stage 7, treated as a second extra
        ("Phantasm boss - Yukari Yakumo's theme", 6, (7, True, "Yukari Yakumo", False)),
        # Title / ending / staff roll
        ("Title screen theme", 6, (0, False, None, False)),
        ("Ending theme", 6, (8, False, None, False)),
        ("Good Ending theme", 6, (8, False, None, False)),
        ("Staff roll theme", 6, (9, False, None, False)),
        ("Credits theme", 6, (9, False, None, False)),
        # Midboss themes — character captured but is_boss=False, is_midboss=True
        ("Stage 5 midboss - Sakuya Izayoi's theme", 6, (5, False, "Sakuya Izayoi", True)),
        # SoEW final-stage tweak: TH02 only has 5 stages, so Final → 5.
        ("Final boss - Mima's theme", 5, (5, True, "Mima", False)),
    ],
)
def test_parse_section_heading_recognized(line, final_stage, expected):
    result = parse_section_heading(line, final_stage_number=final_stage)
    assert result is not None, f"expected to parse {line!r}"
    assert (
        result.stage,
        result.is_boss,
        result.character_name,
        result.is_midboss,
    ) == expected


@pytest.mark.parametrize(
    "line",
    [
        "",
        "Arrangements (album)",
        "Arrangements (download)",
        "Also featured on",
        "References",
        "Music",  # too generic / top-level page name
    ],
)
def test_parse_section_heading_rejected(line):
    assert parse_section_heading(line, final_stage_number=6) is None


# ---------------------------------------------------------------------------
# extract_japanese_title
# ---------------------------------------------------------------------------


def test_extract_japanese_title_simple_bold():
    wikitext = "'''月時計　～ ルナ・ダイアル''' (''Tsukidokei ~ Luna Dial'')\n\nMore text."
    assert extract_japanese_title(wikitext) == "月時計　～ ルナ・ダイアル"


def test_extract_japanese_title_strips_wikilinks():
    wikitext = "'''[[Some Theme|月時計]]''' (''Tsukidokei'')"
    assert extract_japanese_title(wikitext) == "月時計"


def test_extract_japanese_title_returns_none_when_no_bold():
    assert extract_japanese_title("just a plain line\n* not bold") is None


# ---------------------------------------------------------------------------
# split_sections_by_heading
# ---------------------------------------------------------------------------


def test_split_sections_by_heading_groups_under_each_heading():
    wikitext = (
        "intro line\n"
        "== Stage 1 theme ==\n"
        "'''テーマA'''\n"
        "stage1 body\n"
        "=== Stage 5 boss - Sakuya Izayoi's theme ===\n"
        "'''月時計'''\n"
        "stage5 body\n"
    )
    sections = split_sections_by_heading(wikitext)
    assert "Stage 1 theme" in sections
    assert "Stage 5 boss - Sakuya Izayoi's theme" in sections
    assert "stage1 body" in sections["Stage 1 theme"]
    assert "stage5 body" in sections["Stage 5 boss - Sakuya Izayoi's theme"]
    # Intro lines come back under the empty-string key
    assert "intro line" in sections[""]


# ---------------------------------------------------------------------------
# parse_spell_card_owners
# ---------------------------------------------------------------------------


def test_parse_spell_card_owners_extracts_owner_param():
    wikitext = (
        "{{Spell Card\n"
        "|number=163\n"
        "|name1=秘法「九字刺し」\n"
        "|owner=Sanae Kochiya\n"
        "|stage=Extra\n"
        "}}\n"
        "{{Spell Card\n"
        "|number=164\n"
        "|owner=Sanae Kochiya\n"
        "}}\n"
        "{{Spell Card\n"
        "|number=170\n"
        "|owner=Utsuho Reiuji\n"
        "}}\n"
    )
    owners = parse_spell_card_owners(wikitext)
    # Order preserved, duplicates kept (caller dedupes to taste)
    assert owners == ["Sanae Kochiya", "Sanae Kochiya", "Utsuho Reiuji"]


def test_parse_spell_card_owners_handles_wikilink_owner():
    wikitext = "{{Spell Card|number=1|owner=[[Sakuya Izayoi]]}}"
    assert parse_spell_card_owners(wikitext) == ["Sakuya Izayoi"]


def test_parse_spell_card_owners_skips_non_spell_card_templates():
    wikitext = (
        "{{Infobox|name=Test}}\n"
        "{{Spell Card|owner=Reimu Hakurei}}\n"
        "{{Other|owner=should be ignored}}\n"
    )
    assert parse_spell_card_owners(wikitext) == ["Reimu Hakurei"]


# ---------------------------------------------------------------------------
# Name comparators
# ---------------------------------------------------------------------------


def test_normalize_japanese_title_unifies_tildes_and_widths():
    a = normalize_japanese_title("月時計　～ ルナ・ダイアル")
    b = normalize_japanese_title("月時計 〜 ルナ・ダイアル")
    c = normalize_japanese_title("月時計 ~ ルナ・ダイアル")
    assert a == b == c


def test_name_token_set_is_order_independent():
    """`Sakuya Izayoi` and `Izayoi Sakuya` collapse to the same set."""
    assert name_token_set("Sakuya Izayoi") == name_token_set("Izayoi Sakuya")
    assert name_token_set("Sakuya Izayoi") != name_token_set("Sakuya Reimu")
