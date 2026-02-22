import os
from contextlib import asynccontextmanager
from typing import Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy import text, func, distinct
from sqlalchemy.orm import Session, selectinload
from apscheduler.schedulers.background import BackgroundScheduler

import database
import models
import schemas
import scraper
from logger import get_logger

load_dotenv()

log = get_logger(__name__)

SCRAPE_API_KEY = os.getenv("SCRAPE_API_KEY", "")
CORS_ORIGINS   = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000").split(",")]


# ---------------------------------------------------------------------------
# API key guard for the /scrape endpoint
# ---------------------------------------------------------------------------

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def _require_scrape_key(api_key: str = Security(_api_key_header)):
    """If SCRAPE_API_KEY is set in .env, the caller must supply it."""
    if SCRAPE_API_KEY and api_key != SCRAPE_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing X-API-Key header")
    return api_key


# ---------------------------------------------------------------------------
# DB helpers (run at startup)
# ---------------------------------------------------------------------------

def _seed_languages():
    """Insert the 6 known language rows if they don't exist yet."""
    db = database.SessionLocal()
    try:
        known = [
            ("Tamil", 9), ("Telugu", 22), ("Hindi", 56),
            ("Malayalam", 34), ("Kannada", 67), ("English", 45),
        ]
        for name, forum_id in known:
            if not db.query(models.Language).filter_by(name=name).first():
                db.add(models.Language(name=name, forum_id=forum_id))
        db.commit()
        log.info("Languages seeded.")
    except Exception as e:
        log.error("Language seed failed: %s", e)
        db.rollback()
    finally:
        db.close()


def _ensure_fulltext_index():
    """
    Create a FULLTEXT index on movies(title, synopsis) if it doesn't exist.
    MySQL raises an error if you try to add a duplicate index — we catch and ignore it.
    """
    db = database.SessionLocal()
    try:
        db.execute(text(
            "ALTER TABLE movies ADD FULLTEXT INDEX ft_title_synopsis (title, synopsis)"
        ))
        db.commit()
        log.info("FULLTEXT index created on movies(title, synopsis).")
    except Exception:
        # Index already exists — safe to ignore
        db.rollback()
    finally:
        db.close()


# ---------------------------------------------------------------------------
# APScheduler — auto scrape every 6 hours
# ---------------------------------------------------------------------------

scheduler = BackgroundScheduler()

def _scheduled_scrape():
    log.info("APScheduler: starting scheduled scrape...")
    db = database.SessionLocal()
    try:
        result = scraper.scrape_and_save_movies(db)
        log.info("APScheduler: %s", result["message"])
    except Exception as e:
        log.error("APScheduler: scrape failed — %s", e)
    finally:
        db.close()

scheduler.add_job(_scheduled_scrape, "interval", hours=6, id="auto_scrape")


# ---------------------------------------------------------------------------
# Lifespan (replaces deprecated @app.on_event)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    models.Base.metadata.create_all(bind=database.engine)
    _seed_languages()
    _ensure_fulltext_index()
    scheduler.start()
    log.info("Server started. Scheduler running.")
    yield
    # Shutdown
    scheduler.shutdown(wait=False)
    log.info("Server shutting down.")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Tamil Movies API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Serializers
# ---------------------------------------------------------------------------

def _to_list_out(m: models.Movie) -> schemas.MovieListOut:
    return schemas.MovieListOut(
        id=m.id,
        title=m.title,
        year=m.year,
        poster_url=m.poster_url,
        backdrop_url=m.backdrop_url,
        tmdb_rating=m.tmdb_rating,
        source_format=m.source_format,
        runtime=m.runtime,
        created_at=m.created_at,
        genres=[mg.genre.name for mg in m.genres],
        languages=[ml.language.name for ml in m.languages],
    )


def _to_detail_out(m: models.Movie) -> schemas.MovieDetailOut:
    return schemas.MovieDetailOut(
        id=m.id,
        title=m.title,
        year=m.year,
        synopsis=m.synopsis,
        director=m.director,
        actors=m.actors,
        poster_url=m.poster_url,
        backdrop_url=m.backdrop_url,
        tmdb_rating=m.tmdb_rating,
        tmdb_id=m.tmdb_id,
        source_format=m.source_format,
        runtime=m.runtime,
        created_at=m.created_at,
        genres=[mg.genre.name for mg in m.genres],
        languages=[ml.language.name for ml in m.languages],
        downloads=[
            schemas.DownloadOut(
                id=dl.id,
                quality=dl.quality,
                codec=dl.codec,
                audio_format=dl.audio_format,
                audio_languages=dl.audio_languages,
                file_size=dl.file_size,
                magnet_url=dl.magnet_url,
                source_type=dl.source_type,
            )
            for dl in m.downloads
        ],
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/api/movies", response_model=schemas.MovieListResponse)
def get_movies(
    db:       Session       = Depends(database.get_db),
    search:   Optional[str] = Query(None,  description="Search by title (FULLTEXT)"),
    genre:    Optional[str] = Query(None,  description="Filter by genre name"),
    language: Optional[str] = Query(None,  description="Filter by language name"),
    quality:  Optional[str] = Query(None,  description="Filter by download quality e.g. 1080p"),
    year:     Optional[int] = Query(None,  description="Filter by release year"),
    format:   Optional[str] = Query(None,  description="Filter by source format e.g. BluRay"),
    cursor:   Optional[int] = Query(None, description="Last seen movie ID for cursor pagination"),
    page:     Optional[int] = Query(None, ge=1, description="Page number for offset pagination (use instead of cursor)"),
    limit:    int           = Query(24, ge=1, le=100),
):
    query = (
        db.query(models.Movie)
        .options(
            selectinload(models.Movie.genres).selectinload(models.MovieGenre.genre),
            selectinload(models.Movie.languages).selectinload(models.MovieLanguage.language),
        )
    )

    # FULLTEXT search (fast on 1M+ rows) — falls back to LIKE for < 3 chars
    if search:
        if len(search) >= 3:
            query = query.filter(
                text("MATCH(movies.title, movies.synopsis) AGAINST(:q IN BOOLEAN MODE)")
                .bindparams(q=f"{search}*")
            )
        else:
            query = query.filter(models.Movie.title.ilike(f"{search}%"))

    if year:
        query = query.filter(models.Movie.year == year)
    if format:
        query = query.filter(models.Movie.source_format.ilike(f"%{format}%"))
    if genre:
        query = (
            query.join(models.MovieGenre, models.MovieGenre.movie_id == models.Movie.id)
                 .join(models.Genre, models.Genre.id == models.MovieGenre.genre_id)
                 .filter(models.Genre.name.ilike(f"%{genre}%"))
        )
    if language:
        query = (
            query.join(models.MovieLanguage, models.MovieLanguage.movie_id == models.Movie.id)
                 .join(models.Language, models.Language.id == models.MovieLanguage.language_id)
                 .filter(models.Language.name.ilike(f"%{language}%"))
        )
    if quality:
        query = (
            query.join(models.MovieDownload, models.MovieDownload.movie_id == models.Movie.id)
                 .filter(models.MovieDownload.quality.ilike(f"%{quality}%"))
        )

    if page:
        # Page-based pagination — returns total count for pagination UI
        total = query.with_entities(func.count(distinct(models.Movie.id))).scalar() or 0
        total_pages = max(1, (total + limit - 1) // limit)
        skip = (page - 1) * limit
        movies = query.order_by(models.Movie.id.desc()).offset(skip).limit(limit).all()
        return schemas.MovieListResponse(
            movies=[_to_list_out(m) for m in movies],
            next_cursor=None,
            count=len(movies),
            total=total,
            total_pages=total_pages,
        )

    # Cursor-based pagination — O(log n) vs OFFSET O(n)
    if cursor:
        query = query.filter(models.Movie.id < cursor)

    movies = query.order_by(models.Movie.id.desc()).limit(limit).all()
    next_cursor = movies[-1].id if len(movies) == limit else None

    return schemas.MovieListResponse(
        movies=[_to_list_out(m) for m in movies],
        next_cursor=next_cursor,
        count=len(movies),
    )


@app.get("/api/movies/{movie_id}", response_model=schemas.MovieDetailOut)
def get_movie(movie_id: int, db: Session = Depends(database.get_db)):
    movie = (
        db.query(models.Movie)
        .options(
            selectinload(models.Movie.genres).selectinload(models.MovieGenre.genre),
            selectinload(models.Movie.languages).selectinload(models.MovieLanguage.language),
            selectinload(models.Movie.downloads),
        )
        .filter(models.Movie.id == movie_id)
        .first()
    )
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found")
    return _to_detail_out(movie)


@app.get("/api/genres", response_model=list[schemas.GenreOut])
def get_genres(db: Session = Depends(database.get_db)):
    return db.query(models.Genre).order_by(models.Genre.name).all()


@app.get("/api/languages", response_model=list[schemas.LanguageOut])
def get_languages(db: Session = Depends(database.get_db)):
    return db.query(models.Language).order_by(models.Language.name).all()


@app.post(
    "/api/scrape",
    response_model=schemas.ScrapeResult,
    dependencies=[Depends(_require_scrape_key)],
)
def trigger_scrape(db: Session = Depends(database.get_db)):
    """
    Manually trigger the scraper.
    Requires X-API-Key header if SCRAPE_API_KEY is set in .env.
    """
    log.info("Manual scrape triggered via API.")
    try:
        return scraper.scrape_and_save_movies(db)
    except Exception as e:
        log.error("Manual scrape failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/health")
def health():
    return {
        "status":    "ok",
        "scheduler": "running" if scheduler.running else "stopped",
    }
