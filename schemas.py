from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class DownloadOut(BaseModel):
    id:              int
    quality:         Optional[str] = None
    codec:           Optional[str] = None
    audio_format:    Optional[str] = None
    audio_languages: Optional[str] = None
    file_size:       Optional[str] = None
    magnet_url:      Optional[str] = None
    source_type:     Optional[str] = None

    class Config:
        from_attributes = True


class MovieListOut(BaseModel):
    """Lightweight response for list/search endpoints — no downloads."""
    id:            int
    title:         str
    year:          Optional[int]   = None
    poster_url:    Optional[str]   = None
    backdrop_url:  Optional[str]   = None
    tmdb_rating:   Optional[float] = None
    source_format: Optional[str]   = None
    runtime:       Optional[int]   = None
    created_at:    Optional[datetime] = None
    genres:        List[str] = []
    languages:     List[str] = []

    class Config:
        from_attributes = True


class MovieDetailOut(MovieListOut):
    """Full response for detail endpoint — includes synopsis, cast, downloads."""
    synopsis:  Optional[str] = None
    director:  Optional[str] = None
    actors:    Optional[str] = None
    tmdb_id:   Optional[int] = None
    downloads: List[DownloadOut] = []

    class Config:
        from_attributes = True


class MovieListResponse(BaseModel):
    """Paginated list response."""
    movies:      List[MovieListOut]
    next_cursor: Optional[int] = None   # cursor mode: pass as ?cursor= for next page
    count:       int
    total:       Optional[int] = None   # page mode: total matching movies
    total_pages: Optional[int] = None   # page mode: total pages


class GenreOut(BaseModel):
    id:   int
    name: str

    class Config:
        from_attributes = True


class LanguageOut(BaseModel):
    id:       int
    name:     str
    forum_id: Optional[int] = None

    class Config:
        from_attributes = True


class ScrapeResult(BaseModel):
    status:                  str
    message:                 str
    movies_added_or_updated: int
