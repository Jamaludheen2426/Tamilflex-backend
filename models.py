from sqlalchemy import (
    Column, Integer, String, Float, Text, TIMESTAMP,
    ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base


class Movie(Base):
    __tablename__ = "movies"
    __table_args__ = (
        UniqueConstraint("source_url", name="uq_movie_source_url"),
        Index("idx_movie_year", "year"),
        Index("idx_movie_tmdb_rating", "tmdb_rating"),
        Index("idx_movie_created_at", "created_at"),
        Index("idx_movie_source_format", "source_format"),
    )

    id            = Column(Integer, primary_key=True, autoincrement=True)
    title         = Column(String(255), nullable=False)
    year          = Column(Integer, nullable=True)
    synopsis      = Column(Text, nullable=True)
    director      = Column(String(255), nullable=True)
    actors        = Column(Text, nullable=True)
    poster_url    = Column(Text, nullable=True)
    backdrop_url  = Column(Text, nullable=True)
    tmdb_rating   = Column(Float, nullable=True)
    tmdb_id       = Column(Integer, nullable=True, index=True)   # NOT unique — same movie can have multiple forum posts
    source_url    = Column(String(500), nullable=True)   # unique forum topic URL
    source_format = Column(String(50), nullable=True)  # BluRay, WEB-DL, HQ PreDVD…
    runtime       = Column(Integer, nullable=True)     # minutes
    created_at    = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at    = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

    genres    = relationship("MovieGenre",    back_populates="movie", cascade="all, delete-orphan")
    languages = relationship("MovieLanguage", back_populates="movie", cascade="all, delete-orphan")
    downloads = relationship("MovieDownload", back_populates="movie", cascade="all, delete-orphan")


class Genre(Base):
    __tablename__ = "genres"

    id   = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)

    movies = relationship("MovieGenre", back_populates="genre")


class Language(Base):
    __tablename__ = "languages"

    id       = Column(Integer, primary_key=True, autoincrement=True)
    name     = Column(String(50), nullable=False, unique=True)
    forum_id = Column(Integer, nullable=True)   # 9=Tamil, 22=Telugu, 56=Hindi…

    movies = relationship("MovieLanguage", back_populates="language")


class MovieGenre(Base):
    __tablename__ = "movie_genres"
    __table_args__ = (
        UniqueConstraint("movie_id", "genre_id", name="uq_movie_genre"),
        Index("idx_mg_genre_id", "genre_id"),
    )

    id       = Column(Integer, primary_key=True, autoincrement=True)
    movie_id = Column(Integer, ForeignKey("movies.id", ondelete="CASCADE"), nullable=False)
    genre_id = Column(Integer, ForeignKey("genres.id", ondelete="CASCADE"), nullable=False)

    movie = relationship("Movie",  back_populates="genres")
    genre = relationship("Genre",  back_populates="movies")


class MovieLanguage(Base):
    __tablename__ = "movie_languages"
    __table_args__ = (
        UniqueConstraint("movie_id", "language_id", name="uq_movie_language"),
        Index("idx_ml_language_id", "language_id"),
    )

    id          = Column(Integer, primary_key=True, autoincrement=True)
    movie_id    = Column(Integer, ForeignKey("movies.id", ondelete="CASCADE"), nullable=False)
    language_id = Column(Integer, ForeignKey("languages.id", ondelete="CASCADE"), nullable=False)

    movie    = relationship("Movie",    back_populates="languages")
    language = relationship("Language", back_populates="movies")


class MovieDownload(Base):
    __tablename__ = "movie_downloads"
    __table_args__ = (
        Index("idx_dl_movie_id", "movie_id"),
        Index("idx_dl_quality",  "quality"),
    )

    id              = Column(Integer, primary_key=True, autoincrement=True)
    movie_id        = Column(Integer, ForeignKey("movies.id", ondelete="CASCADE"), nullable=False)
    quality         = Column(String(20),  nullable=True)   # 4K, 1080p, 720p, 480p, Rip
    codec           = Column(String(20),  nullable=True)   # x264, x265, AVC, HEVC
    audio_format    = Column(String(50),  nullable=True)   # DD+5.1, AAC 2.0
    audio_languages = Column(String(255), nullable=True)   # Tamil + Telugu + Hindi
    file_size       = Column(String(20),  nullable=True)   # 3.3GB
    magnet_url      = Column(Text, nullable=True)
    source_type     = Column(String(20),  nullable=True)   # magnet, gdrive, mega

    movie = relationship("Movie", back_populates="downloads")


# ---------------------------------------------------------------------------
# Run this SQL once after tables are created to add FULLTEXT search index:
#
#   ALTER TABLE movies ADD FULLTEXT INDEX ft_title_synopsis (title, synopsis);
#
# Then search with:
#   WHERE MATCH(title, synopsis) AGAINST ('leo' IN BOOLEAN MODE)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Seed languages (run once via seed_languages() in main.py startup):
#
# INSERT IGNORE INTO languages (name, forum_id) VALUES
#   ('Tamil', 9), ('Telugu', 22), ('Hindi', 56),
#   ('Malayalam', 34), ('Kannada', 67), ('English', 45);
# ---------------------------------------------------------------------------
