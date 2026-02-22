"""
initial_scrape.py — One-time bulk loader for all historical data.

Run once before going live:
  python initial_scrape.py
  python initial_scrape.py --max-pages 200 --workers 4 --batch 50

Strategy:
  Phase 1 — URL Harvest:
    Loop every page of all 6 language category forums.
    Only fetches index pages (fast, ~1s each).
    Collects (url, raw_title, forum_language) tuples.

  Phase 2 — Enrich & Save:
    Uses ThreadPoolExecutor (3-5 workers) to fetch detail pages + TMDB in parallel.
    Commits to DB in batches of 50 movies to keep memory low.
    Resume-safe: skips source_urls already in DB.
"""

import argparse
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy.orm import Session

import database
import models
import parser as title_parser
from logger import get_logger
from scraper import (
    _headers, _get_with_retry, _omdb_enrich,
    _get_or_create_genre, _get_or_create_language,
    _link_genres, _link_languages,
    FORUM_CATEGORIES, USER_AGENTS,
)

log = get_logger(__name__)

load_dotenv()

BASE_URL = os.getenv("SCRAPER_BASE_URL", "https://1tamilmv.earth/")


# ---------------------------------------------------------------------------
# Phase 1 — Harvest all topic URLs from all forum category pages
# ---------------------------------------------------------------------------

def harvest_all_urls(max_pages: int) -> list[tuple[str, str, str]]:
    """
    Returns list of (url, raw_title, forum_language) for every movie topic found.
    Paginates through each of the 6 language forums up to max_pages pages each.
    """
    all_topics: list[tuple[str, str, str]] = []
    seen_urls: set[str] = set()

    log.info(f"\n=== PHASE 1: URL Harvest (up to {max_pages} pages per category) ===\n")

    # Compile filters once outside the loop
    _DOWNLOAD_KEYWORDS = re.compile(
        r"\b(GB|MB|1080p|720p|480p|360p|4K|2160p|1440p|"
        r"BluRay|BDRip|WEB-DL|WEBRip|HDTV|HDRip|DVDRip|DVDScr|"
        r"HQ\.PreDVD|PreDVD|CAMRip|x264|x265|HEVC|AVC)\b",
        re.IGNORECASE,
    )
    _SKIP_KEYWORDS = re.compile(
        r"\b(Official Trailer|Official Teaser|Official Music Video|"
        r"Music Video|Single Track|OST\b|Lyric Video|Audio Song|"
        r"Song Video|Making Of|Behind the Scenes)\b",
        re.IGNORECASE,
    )

    total_forums = len(FORUM_CATEGORIES)
    for forum_idx, category in enumerate(FORUM_CATEGORIES, 1):
        lang       = category["name"]
        forum_id   = category["forum_id"]
        forum_path = category["path"]
        cat_total  = 0

        log.info(f"  [{forum_idx}/{total_forums}] Forum {forum_id} starting...  (global so far: {len(all_topics)})")

        for page in range(1, max_pages + 1):
            if page == 1:
                page_url = BASE_URL.rstrip("/") + "/" + forum_path
            else:
                # IPS forum pagination: append 'page/N/' to the forum path
                page_url = BASE_URL.rstrip("/") + "/" + forum_path + f"page/{page}/"

            try:
                resp = _get_with_retry(page_url)
                soup = BeautifulSoup(resp.content, "html.parser")

                found_on_page = 0
                for a in soup.find_all("a", href=True):
                    href = a["href"].strip()
                    text = re.sub(r"\s+", " ", a.text.strip())
                    if (
                        "index.php?/forums/topic/" in href
                        and len(text) > 5
                        and _DOWNLOAD_KEYWORDS.search(text)
                        and not _SKIP_KEYWORDS.search(text)
                        and href not in seen_urls
                    ):
                        seen_urls.add(href)
                        all_topics.append((href, text, lang))
                        found_on_page += 1

                cat_total += found_on_page
                log.info(f"    Page {page:>4}: {found_on_page:>4} new  |  forum subtotal: {cat_total:>5}  |  GLOBAL TOTAL: {len(all_topics)}")

                if found_on_page == 0:
                    log.info(f"    Forum {forum_id} exhausted at page {page}.")
                    break

                time.sleep(random.uniform(1.0, 2.5))  # light delay between index pages

            except Exception as e:
                log.info(f"    Page {page} error [forum {forum_id}]: {e}")
                break

        log.info(f"  [{forum_idx}/{total_forums}] Forum {forum_id} done — {cat_total} URLs.  Global: {len(all_topics)}\n")

    log.info(f"Total harvested: {len(all_topics)} unique movie URLs across all categories.\n")
    return all_topics


# ---------------------------------------------------------------------------
# Phase 2 worker — fetch one movie detail + TMDB
# ---------------------------------------------------------------------------

def _fetch_one(url: str, raw_title: str, forum_lang: str) -> dict | None:
    try:
        time.sleep(random.uniform(0.5, 1.5))   # reduced delay for speed
        resp = _get_with_retry(url)
        soup = BeautifulSoup(resp.content, "html.parser")

        parsed = title_parser.parse_title(raw_title)

        poster_url = None
        magnets    = []
        post = soup.select_one("div.ipsType_normal.ipsType_richText")
        if post:
            img = post.select_one("img")
            if img:
                poster_url = img.get("data-src") or img.get("src")
            magnets = [a["href"] for a in post.select('a[href^="magnet:"]')]

        # Skip OMDB — it returns "no result" for ~100% of Tamil movies and adds
        # 2-3 seconds per movie. Re-enable later for targeted enrichment.
        downloads = title_parser.build_downloads(magnets, parsed)
        languages = list(set(
            title_parser.parse_languages_from_title(raw_title) + [forum_lang]
        ))

        return {
            "movie": {
                "title":         parsed["title"],
                "year":          parsed["year"],
                "synopsis":      None,
                "director":      None,
                "actors":        None,
                "poster_url":    poster_url,
                "backdrop_url":  None,
                "tmdb_rating":   None,
                "tmdb_id":       None,
                "source_url":    url,
                "source_format": parsed["source_format"],
                "runtime":       None,
            },
            "genres":    [],
            "languages": languages,
            "downloads": downloads,
        }
    except Exception as e:
        log.error("Worker failed [%s]: %s", url[:70], e)
        return None


# ---------------------------------------------------------------------------
# Phase 2 — Save a batch of fetched movie records to DB
# ---------------------------------------------------------------------------

def _save_batch(db: Session, batch: list[dict], genre_cache: dict, language_cache: dict) -> int:
    saved = 0
    for data in batch:
        try:
            movie_obj = db.query(models.Movie).filter(
                models.Movie.source_url == data["movie"]["source_url"]
            ).first()

            if movie_obj:
                continue  # already in DB

            movie_obj = models.Movie(**data["movie"])
            db.add(movie_obj)
            db.flush()  # get movie_obj.id without committing

            _link_genres(db, movie_obj.id, data["genres"], genre_cache)
            _link_languages(db, movie_obj.id, data["languages"], language_cache)

            for dl in data["downloads"]:
                db.add(models.MovieDownload(movie_id=movie_obj.id, **dl))

            saved += 1
        except Exception as e:
            log.error("DB insert failed for '%s': %s", data['movie'].get('title', '?'), e)
            db.rollback()
            return saved  # stop this batch on error

    try:
        db.commit()
    except Exception as e:
        log.error("Batch commit failed: %s", e)
        db.rollback()

    return saved


# ---------------------------------------------------------------------------
# Phase 2 — Parallel enrichment + batched DB saves
# ---------------------------------------------------------------------------

def enrich_and_save(db: Session, topics: list[tuple[str, str, str]], workers: int, batch_size: int):
    log.info(f"=== PHASE 2: Enrich & Save ({workers} workers, batch size {batch_size}) ===\n")

    # Skip already-scraped URLs
    existing: set[str] = set(
        row[0] for row in
        db.query(models.Movie.source_url).filter(models.Movie.source_url.isnot(None)).all()
    )
    pending = [(u, t, l) for u, t, l in topics if u not in existing]
    log.info(f"  Already in DB: {len(existing)}  |  To process: {len(pending)}\n")

    genre_cache:    dict = {}
    language_cache: dict = {}
    total_saved = 0
    batch: list[dict] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_fetch_one, url, title, lang): (url, title, lang)
            for url, title, lang in pending
        }

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if result:
                batch.append(result)

            if len(batch) >= batch_size:
                saved = _save_batch(db, batch, genre_cache, language_cache)
                total_saved += saved
                log.info(f"  Batch saved: {saved}/{len(batch)} — running total: {total_saved}/{len(pending)}")
                batch = []

    # Final partial batch
    if batch:
        saved = _save_batch(db, batch, genre_cache, language_cache)
        total_saved += saved
        log.info(f"  Final batch saved: {saved}/{len(batch)}")

    log.info(f"\n=== DONE: {total_saved} movies saved to DB ===\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bulk initial scrape — run once before going live.")
    parser.add_argument("--max-pages", type=int, default=100,
                        help="Max index pages to harvest per language category (default: 100)")
    parser.add_argument("--workers",   type=int, default=3,
                        help="Parallel workers for detail page fetching (default: 3, max recommended: 5)")
    parser.add_argument("--batch",     type=int, default=50,
                        help="DB commit batch size (default: 50)")
    parser.add_argument("--reset-db", action="store_true",
                        help="DROP and recreate all tables before scraping (use when schema changed)")
    args = parser.parse_args()

    log.info("=== 1TamilMV Bulk Scraper — Initial Load ===")
    log.info(f"  Max pages per category : {args.max_pages}")
    log.info(f"  Parallel workers       : {args.workers}")
    log.info(f"  DB batch size          : {args.batch}")
    log.info(f"  Reset DB               : {args.reset_db}")

    if args.reset_db:
        log.info("  Dropping all existing tables...")
        models.Base.metadata.drop_all(bind=database.engine)
        log.info("  Tables dropped. Recreating with new schema...")

    models.Base.metadata.create_all(bind=database.engine)

    db = database.SessionLocal()
    try:
        all_topics = harvest_all_urls(max_pages=args.max_pages)
        if all_topics:
            enrich_and_save(db, all_topics, workers=args.workers, batch_size=args.batch)
        else:
            log.info("No URLs found. Check SCRAPER_BASE_URL in .env or site may be down.")
    finally:
        db.close()
