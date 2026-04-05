"""Pydantic models for TouhouDB REST API responses.

TouhouDB is a fork of VocaDB; its response shapes mirror the VocaDB v1 API
contract.  Only fields consumed by LOTAD are declared; unknown fields are
silently ignored (model_config extra='ignore').
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field


class _Base(BaseModel):
    model_config = {"extra": "ignore"}


# ---------------------------------------------------------------------------
# Shared building blocks
# ---------------------------------------------------------------------------


class TagSummary(_Base):
    id: int
    name: str
    categoryName: str = ""


class TagVote(_Base):
    count: int
    tag: TagSummary


class PvInfo(_Base):
    pvId: str  # e.g. YouTube video ID
    service: str  # "Youtube", "NicoNicoDouga", "Bilibili", …
    pvType: str  # "Original", "Reprint", "Other"
    name: str | None = None
    url: str | None = None


class ArtistSummary(_Base):
    id: int
    name: str
    artistType: str = "Unknown"
    additionalNames: str = ""


# ---------------------------------------------------------------------------
# Song models
# ---------------------------------------------------------------------------


class ArtistForSong(_Base):
    """A single artist credit on a song (matches VocaDB ArtistForSongContract)."""

    id: int | None = None
    artist: ArtistSummary | None = None
    # ``name`` is a per-credit display-name override (e.g. guest alias).
    name: str = ""
    # Comma-separated role flags: "Arranger", "Vocalist", "Lyricist", etc.
    # "Default" means the main role for the song type (Arranger for arrangements).
    roles: str = "Default"
    isSupport: bool = False

    @property
    def role_list(self) -> list[str]:
        return [r.strip() for r in self.roles.split(",") if r.strip()]


class AlbumSummary(_Base):
    id: int
    name: str
    discType: str = "Album"


class SongSummary(_Base):
    """Minimal song info, used in album tracks and search results."""

    id: int
    name: str
    songType: str = "Arrangement"
    additionalNames: str = ""


class SongDetail(_Base):
    """Full song detail returned by GET /api/songs/{id}."""

    id: int
    name: str
    additionalNames: str = ""
    songType: str = "Arrangement"
    lengthSeconds: int | None = None
    publishDate: str | None = None  # ISO-8601 datetime string, e.g. "2020-06-14T00:00:00"
    minMilliBpm: int | None = None
    maxMilliBpm: int | None = None
    originalVersionId: int | None = None
    artistString: str = ""  # human-readable "circle feat. vocalist"
    artists: list[ArtistForSong] = Field(default_factory=list)
    albums: list[AlbumSummary] = Field(default_factory=list)
    tags: list[TagVote] = Field(default_factory=list)
    pvs: list[PvInfo] = Field(default_factory=list)

    @property
    def has_lyrics(self) -> bool:
        """True when at least one artist is credited as Vocalist."""
        for credit in self.artists:
            if "Vocalist" in credit.role_list or credit.roles == "Vocalist":
                return True
        return False

    @property
    def is_original_composition(self) -> bool:
        return self.songType in ("Original", "Instrumental") and self.originalVersionId is None

    def youtube_pv_ids(self) -> list[str]:
        """Return YouTube video IDs linked to this song (all PV types)."""
        return [pv.pvId for pv in self.pvs if pv.service.lower() == "youtube"]


class SongSearchResult(_Base):
    """One item in the list returned by GET /api/songs (search)."""

    id: int
    name: str
    songType: str = "Arrangement"
    artistString: str = ""


# ---------------------------------------------------------------------------
# Album models
# ---------------------------------------------------------------------------


class ReleaseDate(_Base):
    year: int | None = None
    month: int | None = None
    day: int | None = None

    def to_date(self) -> date | None:
        """Convert to Python date; returns None if any component is missing."""
        if self.year and self.month and self.day:
            try:
                return date(self.year, self.month, self.day)
            except ValueError:
                return None
        return None


class ArtistForAlbum(_Base):
    id: int | None = None
    artist: ArtistSummary | None = None
    name: str = ""
    roles: str = "Default"
    isSupport: bool = False

    @property
    def role_list(self) -> list[str]:
        return [r.strip() for r in self.roles.split(",") if r.strip()]


class AlbumTrack(_Base):
    id: int | None = None
    trackNumber: int | None = None
    discNumber: int = 1
    name: str = ""
    song: SongSummary | None = None


class AlbumDetail(_Base):
    """Full album detail returned by GET /api/albums/{id}."""

    id: int
    name: str
    additionalNames: str = ""
    discType: str = "Album"
    releaseDate: ReleaseDate | None = None
    catalogNumber: str | None = None
    barcode: str | None = None
    description: str | None = None
    artistString: str = ""
    artists: list[ArtistForAlbum] = Field(default_factory=list)
    tags: list[TagVote] = Field(default_factory=list)
    tracks: list[AlbumTrack] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Artist models
# ---------------------------------------------------------------------------


class ArtistDetail(_Base):
    """Full artist detail returned by GET /api/artists/{id}."""

    id: int
    name: str
    additionalNames: str = ""
    artistType: str = "Unknown"
    description: str | None = None
    groups: list[ArtistSummary] = Field(default_factory=list)
    tags: list[TagVote] = Field(default_factory=list)
    touhoudbUrl: str | None = None


# ---------------------------------------------------------------------------
# Paginated list wrapper
# ---------------------------------------------------------------------------


class SongList(_Base):
    items: list[SongSearchResult] = Field(default_factory=list)
    totalCount: int = 0
