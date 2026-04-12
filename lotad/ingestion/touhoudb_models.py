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
    urlSlug: str = ""
    additionalNames: str = ""


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
    # ``roles`` is the raw stored value — almost always "Default" unless an
    # explicit role override was set by the TouhouDB editor.
    roles: str = "Default"
    # ``effectiveRoles`` is computed by TouhouDB: when ``roles`` is "Default"
    # it resolves the actual role from the artist's ``artistType``
    # (e.g. artistType="Lyricist" → effectiveRoles="Lyricist").
    # This is the field we should use for mapping — not ``roles``.
    effectiveRoles: str = "Default"
    isSupport: bool = False
    # ``categories`` groups credits by function: "Producer", "Vocalist", "Subject", etc.
    # Characters credited as subjects have categories="Subject".
    categories: str = ""

    @property
    def role_list(self) -> list[str]:
        """Comma-separated effectiveRoles split into a list."""
        return [r.strip() for r in self.effectiveRoles.split(",") if r.strip()]


class AlbumSummary(_Base):
    id: int
    name: str
    discType: str = "Album"
    # Additional fields returned when albums appear in a song's album list
    defaultName: str = ""
    releaseDate: ReleaseDate | None = None
    catalogNumber: str | None = None


class SongSummary(_Base):
    """Minimal song info, used in album tracks and search results."""

    id: int
    name: str
    songType: str = "Arrangement"
    additionalNames: str = ""


class SongNotes(_Base):
    """Bilingual notes object returned on song detail responses."""

    english: str = ""
    original: str = ""  # usually the Japanese/romanized text

    def all_text(self) -> str:
        """Combined text of both fields for regex scanning."""
        return f"{self.english}\n{self.original}"


class WebLink(_Base):
    """An external link attached to a song (official site, reference, etc.)."""

    url: str = ""
    category: str = ""  # "Official", "Reference", "Other", etc.
    description: str = ""
    disabled: bool = False


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
    # notes is a structured object {english, original}; requires fields=Notes or always returned
    notes: SongNotes | None = None
    artistString: str = ""  # human-readable "circle feat. vocalist"
    artists: list[ArtistForSong] = Field(default_factory=list)
    albums: list[AlbumSummary] = Field(default_factory=list)
    tags: list[TagVote] = Field(default_factory=list)
    pvs: list[PvInfo] = Field(default_factory=list)
    webLinks: list[WebLink] = Field(default_factory=list)  # requires fields=WebLinks

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


class ReleaseEvent(_Base):
    id: int
    name: str
    urlSlug: str = ""
    category: str = ""


class AlbumDetail(_Base):
    """Full album detail returned by GET /api/albums/{id}."""

    id: int
    name: str
    additionalNames: str = ""  # requires AdditionalNames in fields param
    discType: str = "Album"
    releaseDate: ReleaseDate | None = None
    catalogNumber: str | None = None
    barcode: str | None = None
    description: str | None = None  # requires Description in fields param
    artistString: str = ""
    artists: list[ArtistForAlbum] = Field(default_factory=list)
    tags: list[TagVote] = Field(default_factory=list)
    tracks: list[AlbumTrack] = Field(default_factory=list)
    # Returned as top-level list when ReleaseEvent is in the fields param.
    # (The API also returns a singular releaseEvent; we use the list form.)
    releaseEvents: list[ReleaseEvent] = Field(default_factory=list)


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


class SongDetailList(_Base):
    """Paginated list of full song detail, returned by /api/songs with fields."""

    items: list[SongDetail] = Field(default_factory=list)
    totalCount: int = 0


# ---------------------------------------------------------------------------
# Song-list import models  (GET /api/songLists/import*)
#
# TouhouDB's playlist-import feature lives under /api/songLists/, not
# /api/songs/.  Both endpoints are marked [ApiExplorerSettings(IgnoreApi=true)]
# in VocaDB so they do not appear in Swagger, but they are stable browser
# endpoints used by the "Create song list from YouTube playlist" UI.
#
# Endpoint 1 (first page + metadata):
#   GET /api/songLists/import?url=<youtube-playlist-url>&parseAll=true
#   → ImportedSongList
#
# Endpoint 2 (subsequent pages):
#   GET /api/songLists/import-songs?url=<url>&pageToken=<token>&maxResults=<n>
#   → PartialImportedSongs
# ---------------------------------------------------------------------------


class ImportedSongInList(_Base):
    """One item returned by the songLists import endpoints."""

    # SongForApiContract (basic song info) when TouhouDB found a match, else None.
    # We model it as SongSummary — we only need the id to call get_song() for
    # full detail (artists/tags/albums are not returned by the import endpoint).
    matchedSong: SongSummary | None = None
    name: str = ""  # video title as returned by YouTube
    pvId: str = ""  # YouTube video ID (11-char)
    pvService: str = "Youtube"
    sortIndex: int = 0  # 1-based position in the playlist


class PartialImportedSongs(_Base):
    """Paginated songs response from GET /api/songLists/import-songs."""

    items: list[ImportedSongInList] = Field(default_factory=list)
    totalCount: int = 0
    nextPageToken: str | None = None


class ImportedSongList(_Base):
    """Top-level response from GET /api/songLists/import."""

    name: str = ""  # playlist title from YouTube
    songs: PartialImportedSongs = Field(default_factory=PartialImportedSongs)
