"""
parser.py — Utilities to extract structured data from 1tamilmv forum topic titles.

Title format examples:
  "The Long Walk (2025) (BluRay + Org Auds) - [1080p & 720p - x264 - (Tamil + Telugu + Hindi + Eng) - 3.3GB & 1.4GB | x264 - Tamil - 450MB]"
  "Seetha Payanam (2026) Tamil HQ PreDVD - [1080p & 720p - x264 - 2.6GB & 1.4GB & 900MB]"
  "Ghilli (2004) Tamil TRUE WEB-DL - [1080p & 720p - AVC HEVC - DD+5.1 - 640Kbps - 12.2GB & 3.2GB]"
"""

import re
from datetime import datetime

# -------------------------------------------------------------------
# Lookup lists (order matters — longer/more specific strings first)
# -------------------------------------------------------------------

SOURCE_FORMATS = [
    "TRUE WEB-DL", "WEB-DL", "BluRay", "Blu-Ray",
    "HQ PreDVD", "PreDVD", "HDCAM", "HDTV", "DVDRip",
    "WEBRip", "UHD", "CAM",
]

QUALITY_LABELS = ["4K", "2160p", "1080p", "720p", "480p", "360p"]

CODEC_LABELS = ["HEVC", "x265", "x264", "AVC", "H.265", "H.264"]

AUDIO_FORMAT_LABELS = [
    "DD+5.1", "DDP5.1", "DD5.1", "Atmos", "TrueHD", "DTS",
    "AAC 2.0", "AAC",
]

# Languages found both in category forums and inside title text
LANGUAGE_LABELS = [
    ("Tamil",     r"\bTamil\b"),
    ("Telugu",    r"\bTelugu\b"),
    ("Hindi",     r"\bHindi\b"),
    ("Malayalam", r"\bMalayalam\b"),
    ("Kannada",   r"\bKannada\b"),
    ("English",   r"\bEng(?:lish)?\b"),
]


# -------------------------------------------------------------------
# Public API
# -------------------------------------------------------------------

def parse_title(raw: str) -> dict:
    """
    Parse a raw forum topic title into structured fields.

    Returns:
        title         : str   — clean movie name
        year          : int | None
        source_format : str   — BluRay / WEB-DL / HQ PreDVD / Unknown
        qualities     : list  — ["1080p", "720p", "480p"] in order found
        codec         : str   — first codec found (x264 / x265 / HEVC / …)
        audio_format  : str   — DD+5.1 / AAC / …
        audio_languages: str  — "Tamil + Telugu + Hindi"
        file_sizes    : list  — ["3.3GB", "1.4GB", "450MB"] in order found
    """
    # --- clean movie name ---
    name_match = re.match(r"^(.+?)(?=\s*\(\d{4}\)|\s*-\s*\[)", raw)
    name = name_match.group(1).strip() if name_match else raw.split("(")[0].strip()
    name = re.sub(r"\[.*?\]", "", name)       # remove remaining bracket blocks
    name = re.sub(r"\((?!\d{4}\))[^)]*\)", "", name)  # remove non-year parens
    name = re.sub(r"\s+", " ", name).strip()

    # --- year ---
    y = re.search(r"\((\d{4})\)", raw)
    year = int(y.group(1)) if y else None

    # --- source format ---
    source_format = "Unknown"
    for fmt in SOURCE_FORMATS:
        if re.search(re.escape(fmt), raw, re.IGNORECASE):
            source_format = fmt
            break

    # --- quality tiers (in order of appearance) ---
    qualities = []
    for q in QUALITY_LABELS:
        if q.lower() in raw.lower() and q not in qualities:
            qualities.append(q)

    # --- codec ---
    codec = None
    for c in CODEC_LABELS:
        if re.search(re.escape(c), raw, re.IGNORECASE):
            codec = c
            break

    # --- audio format ---
    audio_format = None
    for af in AUDIO_FORMAT_LABELS:
        if re.search(re.escape(af), raw, re.IGNORECASE):
            audio_format = af
            break

    # --- audio languages ---
    found = []
    for lang_name, pattern in LANGUAGE_LABELS:
        if re.search(pattern, raw, re.IGNORECASE) and lang_name not in found:
            found.append(lang_name)
    audio_languages = " + ".join(found)

    # --- file sizes (all occurrences, in order) ---
    file_sizes = re.findall(r"(\d+(?:\.\d+)?\s*(?:GB|MB))", raw, re.IGNORECASE)
    file_sizes = [s.strip() for s in file_sizes]

    return {
        "title":          name,
        "year":           year,
        "source_format":  source_format,
        "qualities":      qualities,
        "codec":          codec,
        "audio_format":   audio_format,
        "audio_languages": audio_languages,
        "file_sizes":     file_sizes,
    }


def parse_languages_from_title(raw: str) -> list[str]:
    """Return list of language names found in the raw title string."""
    found = []
    for lang_name, pattern in LANGUAGE_LABELS:
        if re.search(pattern, raw, re.IGNORECASE) and lang_name not in found:
            found.append(lang_name)
    return found


def build_downloads(magnets: list[str], parsed: dict) -> list[dict]:
    """
    Match each magnet link to a quality/size from the parsed title.

    The forum post lists downloads in the same order as the title specifies them
    (1080p first, then 720p, then Rip/small versions).

    Returns list of dicts ready for MovieDownload insertion.
    """
    qualities  = parsed["qualities"]  or []
    file_sizes = parsed["file_sizes"] or []
    codec      = parsed["codec"]      or ""
    audio_fmt  = parsed["audio_format"]    or ""
    audio_lang = parsed["audio_languages"] or ""

    downloads = []
    for i, magnet in enumerate(magnets):
        quality   = qualities[i]  if i < len(qualities)  else "Rip"
        file_size = file_sizes[i] if i < len(file_sizes) else "Unknown"
        downloads.append({
            "quality":         quality,
            "codec":           codec,
            "audio_format":    audio_fmt,
            "audio_languages": audio_lang,
            "file_size":       file_size,
            "magnet_url":      magnet,
            "source_type":     "magnet",
        })
    return downloads
