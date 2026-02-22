"""
scraper.py — Lightweight CRON scraper. Called every 6 hours by APScheduler.

Strategy:
  - Iterates all 6 language category forums
  - Scrapes only page 1 of each (newest posts are always at top)
  - Stops per-forum the moment it hits a source_url already in the DB
  - Inserts into normalized tables: movies, movie_genres, movie_languages, movie_downloads
  - Enriches with OMDB (IMDb data) for genre, rating, synopsis, director, actors, poster

For full historical bulk load, run initial_scrape.py instead.
"""

import os
import random
import re
import time
from datetime import datetime
from functools import lru_cache

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy.orm import Session

import models
import parser as title_parser
from logger import get_logger

load_dotenv()

log = get_logger(__name__)

BASE_URL          = os.getenv("SCRAPER_BASE_URL", "https://1tamilmv.earth/")
OMDB_API_KEY      = os.getenv("OMDB_API_KEY", "")
TMDB_BEARER_TOKEN = os.getenv("TMDB_BEARER_TOKEN", "")

# Tamil-only sub-forums (sub-categories under forum 9 - Tamil Language)
# Each sub-forum contains the actual movie download topics with pagination.
FORUM_CATEGORIES = [
    {"name": "Tamil", "forum_id": 10, "path": "index.php?/forums/forum/10-predvd-dvdscr-cam-tc/"},
    {"name": "Tamil", "forum_id": 11, "path": "index.php?/forums/forum/11-web-hd-itunes-hd-bluray/"},
    {"name": "Tamil", "forum_id": 12, "path": "index.php?/forums/forum/12-hd-rips-dvd-rips-br-rips/"},
    {"name": "Tamil", "forum_id": 13, "path": "index.php?/forums/forum/13-dvd9-dvd5/"},
    {"name": "Tamil", "forum_id": 14, "path": "index.php?/forums/forum/14-hdtv-sdtv-hdtv-rips/"},
    {"name": "Tamil", "forum_id": 19, "path": "index.php?/forums/forum/19-web-series-tv-shows/"},
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


def _headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Referer": BASE_URL,
    }


def _get_with_retry(url: str, retries: int = 3, timeout: int = 15) -> requests.Response:
    """GET with exponential backoff. Raises on final failure."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=_headers(), timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == retries:
                raise
            wait = 2 ** attempt   # 2s, 4s, 8s
            log.warning("Request failed (attempt %d/%d): %s — retrying in %ds", attempt, retries, e, wait)
            time.sleep(wait)


# ---------------------------------------------------------------------------
# OMDB enrichment (IMDb data — free, 1000 req/day)
# Sign up: http://www.omdbapi.com/apikey.aspx
# ---------------------------------------------------------------------------

def _omdb_enrich(title: str, year: int | None) -> dict:
    if not OMDB_API_KEY or OMDB_API_KEY == "your_omdb_api_key_here":
        return {}
    try:
        params = {"t": title, "apikey": OMDB_API_KEY, "plot": "full", "type": "movie"}
        if year:
            params["y"] = year

        res  = requests.get("http://www.omdbapi.com/", params=params, timeout=10)
        data = res.json()

        # Retry without year if not found
        if data.get("Response") == "False" and year:
            params.pop("y")
            res  = requests.get("http://www.omdbapi.com/", params=params, timeout=10)
            data = res.json()

        if data.get("Response") == "False":
            log.warning("OMDB: no result for '%s' (%s)", title, year)
            return {}

        # Parse runtime: "165 min" → 165
        runtime = None
        rt = data.get("Runtime", "")
        if "min" in rt:
            try:
                runtime = int(rt.replace("min", "").strip())
            except ValueError:
                pass

        # Parse rating: "7.3" → 7.3
        try:
            rating = float(data.get("imdbRating", "0") or "0")
        except ValueError:
            rating = 0.0

        # Genres: "Action, Crime, Thriller" → ["Action", "Crime", "Thriller"]
        genres = [
            g.strip() for g in data.get("Genre", "").split(",")
            if g.strip() and g.strip() != "N/A"
        ]

        def _clean(val: str) -> str:
            return val if val and val != "N/A" else ""

        return {
            "tmdb_id":     None,   # OMDB uses imdbID, not stored separately
            "tmdb_rating": round(rating, 1),
            "synopsis":    _clean(data.get("Plot", "")),
            "director":    _clean(data.get("Director", "")),
            "actors":      _clean(data.get("Actors", "")),
            "runtime":     runtime,
            "poster_url":  _clean(data.get("Poster", "")),
            "backdrop_url": None,   # OMDB doesn't provide backdrop images
            "genres":      genres,
        }
    except Exception as e:
        log.warning("OMDB failed for '%s': %s", title, e)
        return {}


# ---------------------------------------------------------------------------
# TMDB enrichment (better poster + backdrop coverage)
# ---------------------------------------------------------------------------

_tmdb_session = requests.Session()
_tmdb_session.verify = False

@lru_cache(maxsize=256)
def _tmdb_enrich(title: str, year: int | None) -> dict:
    """Fetch poster, backdrop and metadata from TMDB. Results are cached."""
    if not TMDB_BEARER_TOKEN:
        return {}
    try:
        headers = {
            "Authorization": f"Bearer {TMDB_BEARER_TOKEN}",
            "accept": "application/json",
        }
        base_url = "https://api.themoviedb.org/3/search/movie"

        def get_results(params):
            r = _tmdb_session.get(base_url, headers=headers, params=params, timeout=10)
            if r.status_code == 200:
                return r.json().get("results", [])
            return []

        params = {"query": title}
        if year and year < 2026:
            params["primary_release_year"] = year
        results = get_results(params)

        if not results and year:
            results = get_results({"query": title})

        if not results:
            return {}

        movie = results[0]
        poster_path   = movie.get("poster_path")
        backdrop_path = movie.get("backdrop_path")

        try:
            rating = round(float(movie.get("vote_average", 0) or 0), 1) or None
        except (ValueError, TypeError):
            rating = None

        return {
            "tmdb_id":      movie.get("id"),
            "poster_url":   f"https://image.tmdb.org/t/p/w500{poster_path}"   if poster_path   else None,
            "backdrop_url": f"https://image.tmdb.org/t/p/w1280{backdrop_path}" if backdrop_path else None,
            "tmdb_rating":  rating,
            "synopsis":     movie.get("overview") or None,
        }
    except Exception as e:
        log.warning("TMDB failed for '%s': %s", title, e)
        return {}


def _enrich(title: str, year: int | None) -> dict:
    """Try TMDB first (better images), fall back to OMDB."""
    tmdb = _tmdb_enrich(title, year)
    if tmdb.get("poster_url"):
        # Merge OMDB genres/director/actors if TMDB doesn't have them
        omdb = _omdb_enrich(title, year)
        return {**omdb, **tmdb}   # TMDB values win
    return _omdb_enrich(title, year)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_or_create_genre(db: Session, name: str, cache: dict) -> models.Genre:
    if name not in cache:
        obj = db.query(models.Genre).filter_by(name=name).first()
        if not obj:
            obj = models.Genre(name=name)
            db.add(obj)
            db.flush()
        cache[name] = obj
    return cache[name]


def _get_or_create_language(db: Session, name: str, cache: dict) -> models.Language:
    if name not in cache:
        obj = db.query(models.Language).filter_by(name=name).first()
        if not obj:
            obj = models.Language(name=name)
            db.add(obj)
            db.flush()
        cache[name] = obj
    return cache[name]


def _link_genres(db, movie_id, genre_names, genre_cache):
    for name in genre_names:
        if not name:
            continue
        genre = _get_or_create_genre(db, name, genre_cache)
        if not db.query(models.MovieGenre).filter_by(movie_id=movie_id, genre_id=genre.id).first():
            db.add(models.MovieGenre(movie_id=movie_id, genre_id=genre.id))


def _link_languages(db, movie_id, lang_names, lang_cache):
    for name in lang_names:
        if not name:
            continue
        lang = _get_or_create_language(db, name, lang_cache)
        if not db.query(models.MovieLanguage).filter_by(movie_id=movie_id, language_id=lang.id).first():
            db.add(models.MovieLanguage(movie_id=movie_id, language_id=lang.id))


# ---------------------------------------------------------------------------
# Fetch + parse a single movie detail page
# ---------------------------------------------------------------------------

def _fetch_movie_detail(url: str, raw_title: str, forum_lang: str) -> dict | None:
    try:
        time.sleep(random.uniform(2.0, 4.0))
        resp     = _get_with_retry(url)
        soup     = BeautifulSoup(resp.content, "html.parser")
        parsed   = title_parser.parse_title(raw_title)

        poster_url = None
        magnets    = []
        post = soup.select_one("div.ipsType_normal.ipsType_richText")
        if post:
            img = post.select_one("img")
            if img:
                poster_url = img.get("data-src") or img.get("src")
            magnets = [a["href"] for a in post.select('a[href^="magnet:"]')]

        tmdb      = _enrich(parsed["title"], parsed["year"])
        downloads = title_parser.build_downloads(magnets, parsed)
        languages = list(set(
            title_parser.parse_languages_from_title(raw_title) + [forum_lang]
        ))

        return {
            "movie": {
                "title":         parsed["title"],
                "year":          parsed["year"] or datetime.now().year,
                "synopsis":      tmdb.get("synopsis", ""),
                "director":      tmdb.get("director", ""),
                "actors":        tmdb.get("actors", ""),
                "poster_url":    tmdb.get("poster_url") or poster_url,
                "backdrop_url":  tmdb.get("backdrop_url"),
                "tmdb_rating":   tmdb.get("tmdb_rating", 0.0),
                "tmdb_id":       tmdb.get("tmdb_id"),
                "source_url":    url,
                "source_format": parsed["source_format"],
                "runtime":       tmdb.get("runtime"),
            },
            "genres":    tmdb.get("genres", []),
            "languages": languages,
            "downloads": downloads,
        }
    except Exception as e:
        log.error("fetch_movie_detail failed [%s]: %s", url[:70], e)
        return None


# ---------------------------------------------------------------------------
# Main cron entry point
# ---------------------------------------------------------------------------

def scrape_and_save_movies(db: Session):
    existing_urls: set[str] = set(
        row[0] for row in
        db.query(models.Movie.source_url).filter(models.Movie.source_url.isnot(None)).all()
    )
    log.info("Cron scrape started. Known URLs in DB: %d", len(existing_urls))

    genre_cache:    dict = {}
    language_cache: dict = {}
    total_added = 0

    for category in FORUM_CATEGORIES:
        forum_url = BASE_URL.rstrip("/") + "/" + category["path"]
        log.info("[%s] Checking: %s", category["name"], forum_url)

        try:
            resp = _get_with_retry(forum_url)
            soup = BeautifulSoup(resp.content, "html.parser")

            new_topics = []
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                text = re.sub(r"\s+", " ", a.text.strip())
                if "index.php?/forums/topic/" not in href or len(text) < 20:
                    continue
                if "GB" not in text and "MB" not in text and "Rips" not in text:
                    continue
                if href in existing_urls:
                    log.info("[%s] Hit known URL — no more new movies.", category["name"])
                    break
                new_topics.append((href, text))

            log.info("[%s] %d new topic(s) found.", category["name"], len(new_topics))

            for url, raw_title in new_topics:
                data = _fetch_movie_detail(url, raw_title, category["name"])
                if not data:
                    continue

                movie_obj = db.query(models.Movie).filter(
                    models.Movie.source_url == data["movie"]["source_url"]
                ).first()

                if not movie_obj:
                    movie_obj = models.Movie(**data["movie"])
                    db.add(movie_obj)
                    db.flush()
                    log.info("  + Added: %s (%s)", data["movie"]["title"], data["movie"]["year"])
                    total_added += 1
                else:
                    log.info("  = Exists: %s", data["movie"]["title"])
                    continue

                _link_genres(db, movie_obj.id, data["genres"], genre_cache)
                _link_languages(db, movie_obj.id, data["languages"], language_cache)
                for dl in data["downloads"]:
                    db.add(models.MovieDownload(movie_id=movie_obj.id, **dl))

                existing_urls.add(url)

            db.commit()
            time.sleep(random.uniform(2.0, 4.0))

        except Exception as e:
            log.error("[%s] Category failed: %s", category["name"], e)
            db.rollback()

    log.info("Cron scrape done. %d movies added.", total_added)
    return {
        "status": "success",
        "message": f"Cron scrape done. {total_added} new movies added.",
        "movies_added_or_updated": total_added,
    }
