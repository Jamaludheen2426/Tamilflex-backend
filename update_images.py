"""
update_images.py
----------------
Batch-updates poster_url, backdrop_url and related fields using TMDB API.

Usage:
    python update_images.py              # process all movies missing images
    python update_images.py --all        # re-process every movie (overwrite)
    python update_images.py --limit 100  # process only first 100
"""

import argparse
import time
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import urllib3
from dotenv import load_dotenv
from sqlalchemy.orm import Session

import database
import models

load_dotenv()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── TMDB config ───────────────────────────────────────────────────────────────

TMDB_BEARER_TOKEN = os.getenv("TMDB_BEARER_TOKEN", "")
TMDB_SEARCH_URL   = "https://api.themoviedb.org/3/search/movie"
TMDB_TV_SEARCH    = "https://api.themoviedb.org/3/search/tv"
TMDB_DETAIL_URL   = "https://api.themoviedb.org/3/movie/{tmdb_id}"
TMDB_TV_DETAIL    = "https://api.themoviedb.org/3/tv/{tmdb_id}"
TMDB_CREDITS_URL  = "https://api.themoviedb.org/3/movie/{tmdb_id}/credits"
TMDB_TV_CREDITS   = "https://api.themoviedb.org/3/tv/{tmdb_id}/credits"

REQUEST_DELAY = 0.05  # small delay between thread submissions

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {TMDB_BEARER_TOKEN}",
    "accept": "application/json",
})
session.verify = False


# ── TMDB helpers ──────────────────────────────────────────────────────────────

def _search(title: str, year: int | None) -> tuple[dict | None, str]:
    """Returns (result, media_type) where media_type is 'movie' or 'tv'."""
    use_year = year if (year and year < 2026) else None

    def _get(url, params):
        try:
            r = session.get(url, params=params, timeout=15)
            if r.status_code == 200:
                return r.json().get("results", [])
            print(f"    [TMDB {r.status_code}] {r.text[:100]}", flush=True)
        except Exception as e:
            print(f"    [TMDB ERROR] {e}", flush=True)
        return []

    # 1. Movie search with year
    params = {"query": title}
    if use_year:
        params["primary_release_year"] = use_year
    results = _get(TMDB_SEARCH_URL, params)

    # 2. Movie search without year
    if not results and use_year:
        results = _get(TMDB_SEARCH_URL, {"query": title})
    if results:
        return results[0], "movie"

    # 3. TV search with year
    tv_params = {"query": title}
    if use_year:
        tv_params["first_air_date_year"] = use_year
    tv_results = _get(TMDB_TV_SEARCH, tv_params)

    # 4. TV search without year
    if not tv_results and use_year:
        tv_results = _get(TMDB_TV_SEARCH, {"query": title})
    if tv_results:
        return tv_results[0], "tv"

    return None, "movie"


def _detail(tmdb_id: int) -> dict:
    try:
        r = session.get(TMDB_DETAIL_URL.format(tmdb_id=tmdb_id), timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {}


def _credits(tmdb_id: int) -> dict:
    try:
        r = session.get(TMDB_CREDITS_URL.format(tmdb_id=tmdb_id), timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {}


def enrich(title: str, year: int | None) -> dict:
    result, media_type = _search(title, year)
    if not result:
        return {}

    tmdb_id       = result.get("id")
    poster_path   = result.get("poster_path")
    backdrop_path = result.get("backdrop_path")

    data = {
        "tmdb_id":      tmdb_id,
        "poster_url":   f"https://image.tmdb.org/t/p/w500{poster_path}"   if poster_path   else None,
        "backdrop_url": f"https://image.tmdb.org/t/p/w1280{backdrop_path}" if backdrop_path else None,
        "tmdb_rating":  round(result.get("vote_average", 0), 1) or None,
        "synopsis":     result.get("overview") or None,
    }

    if tmdb_id:
        if media_type == "tv":
            detail  = session.get(TMDB_TV_DETAIL.format(tmdb_id=tmdb_id), timeout=15).json() if True else {}
            credits_r = session.get(TMDB_TV_CREDITS.format(tmdb_id=tmdb_id), timeout=15)
            credits = credits_r.json() if credits_r.status_code == 200 else {}
            # TV shows use episode_run_time list
            runtimes = detail.get("episode_run_time", [])
            data["runtime"] = runtimes[0] if runtimes else None
        else:
            detail  = _detail(tmdb_id)
            credits = _credits(tmdb_id)
            data["runtime"] = detail.get("runtime") or None

        directors = [c["name"] for c in credits.get("crew", []) if c.get("job") in ("Director", "Executive Producer")]
        top_cast  = [c["name"] for c in credits.get("cast", [])[:5]]
        data["director"] = ", ".join(directors[:2]) or None
        data["actors"]   = ", ".join(top_cast)      or None

    return data


# ── Main batch ────────────────────────────────────────────────────────────────

def _process_one(movie_id: int, title: str, year: int | None, overwrite: bool) -> dict:
    """Fetch TMDB data for one movie. Runs in a thread."""
    enriched = enrich(title, year)
    return {"id": movie_id, "title": title, "year": year, "enriched": enriched}


def run(overwrite: bool = False, limit: int | None = None, workers: int = 8):
    db: Session = database.SessionLocal()
    try:
        q = db.query(models.Movie)
        if not overwrite:
            from sqlalchemy import or_
            q = q.filter(
                or_(
                    models.Movie.backdrop_url == None,
                    models.Movie.poster_url   == None,
                    models.Movie.poster_url   == "",
                    ~models.Movie.poster_url.like("%tmdb.org%"),
                )
            )
        q = q.order_by(models.Movie.id.asc())
        if limit:
            q = q.limit(limit)

        movies = q.all()
        total  = len(movies)
        print(f"Found {total} movies to process | workers={workers}\n", flush=True)

        # Build a lookup by id for fast access after threads complete
        movie_map = {m.id: m for m in movies}

        updated = skipped = failed = 0
        done = 0

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for movie in movies:
                f = executor.submit(_process_one, movie.id, movie.title, movie.year, overwrite)
                futures[f] = movie.id
                time.sleep(REQUEST_DELAY)  # small stagger to avoid burst

            for future in as_completed(futures):
                done += 1
                result = future.result()
                movie  = movie_map[result["id"]]
                enriched = result["enriched"]
                prefix = f"[{done}/{total}] {result['title']!r} ({result['year']})"

                if not enriched:
                    print(f"  {prefix} → NOT FOUND", flush=True)
                    failed += 1
                    continue

                changed = False

                def _set(field, value, _movie=movie):
                    nonlocal changed
                    cur = getattr(_movie, field)
                    # For image URLs: also replace non-TMDB URLs (broken forum images)
                    is_non_tmdb_img = (
                        field in ("poster_url", "backdrop_url")
                        and cur
                        and "tmdb.org" not in cur
                    )
                    if value and (overwrite or not cur or is_non_tmdb_img):
                        setattr(_movie, field, value)
                        changed = True

                _set("poster_url",   enriched.get("poster_url"))
                _set("backdrop_url", enriched.get("backdrop_url"))
                _set("tmdb_id",      enriched.get("tmdb_id"))
                _set("tmdb_rating",  enriched.get("tmdb_rating"))
                _set("synopsis",     enriched.get("synopsis"))
                _set("runtime",      enriched.get("runtime"))
                _set("director",     enriched.get("director"))
                _set("actors",       enriched.get("actors"))

                if changed:
                    db.commit()
                    updated += 1
                    has_poster = "✓ poster" if enriched.get("poster_url") else "✗ no poster"
                    print(f"  {prefix} → UPDATED ({has_poster})", flush=True)
                else:
                    skipped += 1
                    print(f"  {prefix} → skipped", flush=True)

                if done % 50 == 0:
                    db.commit()
                    print(f"\n  --- {done}/{total} | updated={updated} failed={failed} ---\n", flush=True)

        db.commit()
        print(f"\n{'='*60}", flush=True)
        print(f"Done. Total={total} | Updated={updated} | Skipped={skipped} | Not found={failed}", flush=True)
        print(f"{'='*60}", flush=True)

    except KeyboardInterrupt:
        db.commit()
        print("\nInterrupted. Progress saved.", flush=True)
    finally:
        db.close()


if __name__ == "__main__":
    sys.stdout.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--all",     action="store_true")
    parser.add_argument("--limit",   type=int, default=None)
    parser.add_argument("--workers", type=int, default=8, help="Parallel threads (default 8)")
    args = parser.parse_args()

    if not TMDB_BEARER_TOKEN:
        print("ERROR: TMDB_BEARER_TOKEN missing in .env", flush=True)
        sys.exit(1)

    print(f"TMDB token: {TMDB_BEARER_TOKEN[:20]}...", flush=True)
    print("Starting...\n", flush=True)

    run(overwrite=args.all, limit=args.limit, workers=args.workers)
