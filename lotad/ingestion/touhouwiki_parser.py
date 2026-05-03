"""Pure-function parsers for Touhou Wiki content.

Two responsibilities:

1. Parse a section heading from a ``List_by_Song/<Game>`` page into a
   ``ThemeHeading`` capturing stage number, boss flag, and (when the heading
   names one) the character.
2. Extract the bold Japanese theme name out of a section's wikitext, and
   parse spell-card template parameters out of ``Spell_Cards/<stage>`` pages.

Kept side-effect-free so the unit tests can exercise the heading regex
without touching the network.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

import mwparserfromhell


@dataclass(frozen=True)
class ThemeHeading:
    """A parsed wiki section heading describing one ZUN theme.

    ``character_name`` is the romanized name from the heading (e.g.
    ``"Sakuya Izayoi"``) — None if the heading doesn't name one (e.g.
    plain ``"Stage 5 theme"``).  ``is_boss`` is True only for main-boss
    themes; midboss themes have ``is_boss=False`` and ``is_midboss=True``.
    """

    stage: int
    is_boss: bool
    character_name: str | None
    is_midboss: bool = False


# Strips a trailing ``- X's theme`` (or `` - X theme``) from a heading.  Owner
# names sometimes include apostrophes or dots (``Iku's``, ``Dr. Latency``); the
# greedy ``.+`` keeps the full string up to the literal ``'s theme`` /
# `` theme`` suffix.
_CHARACTER_SUFFIX_RE = re.compile(
    r"\s*[-–—]\s*(?P<char>.+?)(?:'s)?\s+theme\s*$",
    re.IGNORECASE,
)

# Captures ``Stage 5`` / ``Stage 5b`` / ``Stage 6A``.  The trailing letter is
# IN-style A/B route disambiguation; we ignore it for stage number.
_STAGE_PREFIX_RE = re.compile(r"^stage\s+(\d+)[a-z]?\b", re.IGNORECASE)


def parse_section_heading(line: str, *, final_stage_number: int) -> ThemeHeading | None:
    """Parse a wiki section heading into stage / boss / character.

    Returns ``None`` if the heading is not a recognized theme heading
    (e.g. ``Arrangements (album)`` or a top-level page section).
    """
    raw = line.strip()
    if not raw:
        return None

    character: str | None = None
    char_match = _CHARACTER_SUFFIX_RE.search(raw)
    if char_match:
        character = char_match.group("char").strip().strip("\"'")
        prefix = raw[: char_match.start()].rstrip(" -–—\t")
    else:
        prefix = raw
        # Tolerate a trailing bare " theme" with no character.
        if prefix.lower().endswith(" theme"):
            prefix = prefix[: -len(" theme")].rstrip()

    prefix_lower = prefix.lower().strip()

    # Title-screen / menu themes
    if prefix_lower in {"title", "title screen", "main title", "menu", "main menu"}:
        return ThemeHeading(stage=0, is_boss=False, character_name=character)

    # Endings — wiki uses variants like "Ending theme", "Good ending theme",
    # "Bad ending A theme".  Anything containing "ending" maps to stage 8.
    if "ending" in prefix_lower:
        return ThemeHeading(stage=8, is_boss=False, character_name=character)

    # Staff roll / credits
    if "staff roll" in prefix_lower or "credits" in prefix_lower:
        return ThemeHeading(stage=9, is_boss=False, character_name=character)

    # Identify the stage portion of the prefix and strip it off; what remains
    # tells us whether this is a stage / boss / midboss heading.
    stage: int
    rest: str
    stage_match = _STAGE_PREFIX_RE.match(prefix_lower)
    if stage_match:
        stage = int(stage_match.group(1))
        rest = prefix_lower[stage_match.end() :].strip()
    elif prefix_lower.startswith("final"):
        stage = final_stage_number
        rest = prefix_lower[len("final") :].strip()
        if rest.startswith("stage"):
            rest = rest[len("stage") :].strip()
    elif prefix_lower.startswith("extra"):
        stage = 7
        rest = prefix_lower[len("extra") :].strip()
        if rest.startswith("stage"):
            rest = rest[len("stage") :].strip()
    elif prefix_lower.startswith("phantasm"):
        # PCB-only; treated as a second extra (stage 7), matching TouhouDB.
        stage = 7
        rest = prefix_lower[len("phantasm") :].strip()
    else:
        return None

    is_midboss = "midboss" in rest or "mid-boss" in rest or "mid boss" in rest
    is_boss = ("boss" in rest) and not is_midboss

    return ThemeHeading(
        stage=stage,
        is_boss=is_boss,
        character_name=character,
        is_midboss=is_midboss,
    )


# Matches ``'''Bold text'''`` capturing the inside.  Non-greedy so it stops at
# the first closing triple-quote on the line.
_BOLD_RE = re.compile(r"'''(.+?)'''")
# Wiki link: ``[[Target|Display]]`` or ``[[Target]]``.
_WIKI_LINK_RE = re.compile(r"\[\[(?:[^\]|]+\|)?([^\]]+)\]\]")
# Italics inside the bold span.
_ITALIC_RE = re.compile(r"''(.+?)''")


def extract_japanese_title(section_wikitext: str) -> str | None:
    """Pull the first bold Japanese theme name from a section's wikitext.

    The convention on Touhou Wiki is that immediately under a theme heading
    the wikitext gives the Japanese title in bold, optionally followed by a
    parenthesised romanization and translation, e.g.::

        '''月時計　～ ルナ・ダイアル''' (''Tsukidokei ~ Luna Dial'')

    Returns the Japanese title with whitespace normalized.  Returns None if
    no bold span is found in the first ~10 lines (theme tables / arrangement
    lists begin further down).
    """
    head = "\n".join(section_wikitext.splitlines()[:15])
    m = _BOLD_RE.search(head)
    if not m:
        return None
    title = m.group(1)
    title = _WIKI_LINK_RE.sub(r"\1", title)
    title = _ITALIC_RE.sub(r"\1", title)
    title = title.strip().strip("\"'")
    return title or None


def split_sections_by_heading(wikitext: str) -> dict[str, str]:
    """Split a page's wikitext into ``{heading_line: section_body}``.

    The MediaWiki API's ``sections`` array gives heading metadata but not
    per-section bodies; splitting locally avoids a second API round-trip.

    Headings of any level (``==``..``======``) are treated as section
    boundaries.  Content above the first heading is returned under the empty
    string key (and is usually intro / TOC content we don't care about).
    """
    sections: dict[str, str] = {}
    current_heading = ""
    buffer: list[str] = []
    for line in wikitext.splitlines():
        m = re.match(r"^(={2,6})\s*(.+?)\s*\1\s*$", line)
        if m:
            sections[current_heading] = "\n".join(buffer)
            current_heading = m.group(2).strip()
            buffer = []
        else:
            buffer.append(line)
    sections[current_heading] = "\n".join(buffer)
    return sections


def parse_spell_card_owners(wikitext: str) -> list[str]:
    """Return the list of owner names from a ``Spell_Cards/<stage>`` page.

    Owners are read from ``{{Spell Card|...|owner=Name|...}}`` (and the
    occasional ``|character=`` variant) template invocations.  Order is
    preserved and duplicates are kept — callers dedupe to taste.
    """
    code = mwparserfromhell.parse(wikitext)
    owners: list[str] = []
    for tmpl in code.filter_templates():
        # Template name matching is whitespace-insensitive.
        name = str(tmpl.name).strip().lower()
        if name not in {"spell card", "spellcard"}:
            continue
        for param_name in ("owner", "character", "user"):
            if not tmpl.has(param_name):
                continue
            value = str(tmpl.get(param_name).value).strip()
            value = _WIKI_LINK_RE.sub(r"\1", value)
            value = value.strip().strip("\"'")
            if value:
                owners.append(value)
                break
    return owners


# ---------------------------------------------------------------------------
# Name normalization helpers used by the DB-side matcher
# ---------------------------------------------------------------------------


def normalize_japanese_title(s: str) -> str:
    """NFKC-normalize, fold whitespace, and unify tilde / wave-dash variants.

    ``～`` (U+FF5E), ``〜`` (U+301C) and ``~`` ASCII tilde all map to ``~``;
    full-width spaces collapse alongside regular whitespace.  Used as the
    comparator for matching wiki-extracted Japanese titles against
    ``original_songs.name`` rows.
    """
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("〜", "~").replace("～", "~")
    return re.sub(r"\s+", " ", s).strip().lower()


def name_token_set(name: str) -> frozenset[str]:
    """Return a case-folded token set for romanized name comparison.

    Matches across name-order conventions: ``"Sakuya Izayoi"`` and
    ``"Izayoi Sakuya"`` produce the same set, so character names from the
    wiki resolve regardless of which order the DB stores.
    """
    cleaned = re.sub(r"[^a-zA-Z0-9\s'\-]", " ", name)
    return frozenset(t.lower() for t in cleaned.split() if t)
