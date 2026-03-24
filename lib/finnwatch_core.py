"""
finnwatch_core.py – Shared library for the finnwatch cron scripts.

Provides HTTP helpers, database schema, page-parsing utilities, and
high-level crawl/recheck functions used by both fw-crawl and fw-recheck.

Dependencies:
  pip install requests beautifulsoup4
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
import sqlite3
import sys
import time
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode, urljoin, urlparse, parse_qs, urlunparse

import requests
from bs4 import BeautifulSoup


# ─────────────────────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────────────────────

FINNWATCH_DIR    = Path.home() / ".finnwatch"
DB_PATH          = FINNWATCH_DIR / "database" / "finnwatch.db"
LOG_FILE         = FINNWATCH_DIR / "database" / "finnwatch.log"
SEARCH_URLS_FILE = FINNWATCH_DIR / "search_urls.txt"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "nb-NO,nb;q=0.9,no;q=0.8,nn;q=0.7,en-US;q=0.6,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

REQUEST_TIMEOUT  = 30     # seconds per request
MIN_DELAY        = 2.0    # seconds between requests (be polite)
MAX_DELAY        = 5.0
MAX_SEARCH_PAGES = 10     # max search result pages to crawl per run


# ─────────────────────────────────────────────────────────────────────────────
#  Logging
# ─────────────────────────────────────────────────────────────────────────────

# Ensure the database/ directory exists before opening the log file handler.
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
#  Search URL loading
# ─────────────────────────────────────────────────────────────────────────────

def load_search_urls(path: Path = SEARCH_URLS_FILE) -> list:
    """
    Read the search URL list.  Blank lines and lines starting with # are
    ignored.  Raises FileNotFoundError if the file is missing.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Search URLs file not found: {path}\n"
            "Create it with one finn.no search URL per line."
        )
    urls = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    return urls


# ─────────────────────────────────────────────────────────────────────────────
#  Database schema
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_STATEMENTS = [
    # One row per finn.no ad (any category).
    """
    CREATE TABLE IF NOT EXISTS ads (
        finnkode     TEXT PRIMARY KEY,
        url          TEXT NOT NULL,
        ad_type      TEXT,             -- inferred from the URL path, e.g. 'car', 'realestate'
        first_seen   TEXT NOT NULL,   -- ISO-8601 UTC timestamp
        last_checked TEXT,            -- ISO-8601 UTC timestamp
        is_active    INTEGER NOT NULL DEFAULT 1
    )
    """,

    # Full snapshot every time an ad changes (or is first seen).
    #
    # A handful of universally-useful fields are promoted to dedicated columns
    # so you can write plain SQL queries without parsing JSON.  Everything else
    # lives in raw_data (complete JSON of whatever was scraped).
    """
    CREATE TABLE IF NOT EXISTS ad_snapshots (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        finnkode      TEXT    NOT NULL REFERENCES ads(finnkode),
        snapshot_time TEXT    NOT NULL,   -- ISO-8601 UTC
        data_hash     TEXT    NOT NULL,   -- SHA-256 of the normalised JSON, used for change detection

        -- Universal fields present on most finn.no ad types (NULL when not applicable)
        title         TEXT,
        price         INTEGER,            -- NOK
        location      TEXT,
        seller_name   TEXT,
        seller_type   TEXT,               -- 'private' | 'dealer' | 'agency' etc.
        published     TEXT,               -- ad publish date as a string
        num_images    INTEGER,
        description   TEXT,

        -- Complete scraped payload – never truncated, never discarded
        raw_data      TEXT    NOT NULL
    )
    """,

    # Field-level audit trail.
    # One row per changed field per snapshot transition.
    """
    CREATE TABLE IF NOT EXISTS ad_field_changes (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        finnkode   TEXT NOT NULL REFERENCES ads(finnkode),
        changed_at TEXT NOT NULL,   -- ISO-8601 UTC
        field_name TEXT NOT NULL,
        old_value  TEXT,
        new_value  TEXT
    )
    """,

    "CREATE INDEX IF NOT EXISTS idx_snap_finnkode  ON ad_snapshots(finnkode)",
    "CREATE INDEX IF NOT EXISTS idx_snap_time      ON ad_snapshots(snapshot_time)",
    "CREATE INDEX IF NOT EXISTS idx_chg_finnkode   ON ad_field_changes(finnkode)",
    "CREATE INDEX IF NOT EXISTS idx_chg_field      ON ad_field_changes(field_name)",
    "CREATE INDEX IF NOT EXISTS idx_ads_type       ON ads(ad_type)",
]


def get_db(path: Path = DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    for stmt in SCHEMA_STATEMENTS:
        stmt = stmt.strip()
        if stmt:
            db.execute(stmt)
    db.commit()
    return db


# ─────────────────────────────────────────────────────────────────────────────
#  HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

_session: Optional[requests.Session] = None


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(HEADERS)
    return _session


def fetch(url: str, *, pause: bool = True) -> Optional[requests.Response]:
    """GET a URL, returning the Response or None on failure."""
    if pause:
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    try:
        resp = get_session().get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        return resp
    except requests.RequestException as exc:
        log.warning("Fetch failed for %s: %s", url, exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  Page data extraction utilities
# ─────────────────────────────────────────────────────────────────────────────

def _decode_b64_json_scripts(html: str) -> list:
    """
    Finn.no embeds all server-side state as base64-encoded JSON in
    <script type="application/json"> tags (NOT __NEXT_DATA__).
    Return a list of successfully decoded+parsed dicts/lists.
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for tag in soup.find_all("script", type="application/json"):
        raw = (tag.string or "").strip()
        if not raw:
            continue
        # Try as-is JSON first
        try:
            results.append(json.loads(raw))
            continue
        except json.JSONDecodeError:
            pass
        # Try base64 decode (pad to multiple of 4)
        try:
            padding = (4 - len(raw) % 4) % 4
            decoded = base64.b64decode(raw + "=" * padding).decode("utf-8", errors="replace")
            results.append(json.loads(decoded))
        except Exception:
            pass
    return results


def _find_docs_and_meta(blobs: list) -> tuple:
    """
    Walk decoded JSON blobs looking for the TanStack Query hydration blob
    that contains the search result docs.
    Returns (docs_list, metadata_dict).
    """
    for blob in blobs:
        if not isinstance(blob, dict):
            continue
        for query in blob.get("queries", []):
            if not isinstance(query, dict):
                continue
            state_data = query.get("state", {}).get("data", {})
            if not isinstance(state_data, dict):
                continue
            docs = state_data.get("docs", [])
            if docs:
                return docs, state_data.get("metadata", {})
    return [], {}


def extract_next_data(html: str) -> Optional[dict]:
    """Return the parsed __NEXT_DATA__ object if present (legacy fallback)."""
    soup = BeautifulSoup(html, "html.parser")
    tag  = soup.find("script", id="__NEXT_DATA__")
    if tag and tag.string:
        try:
            return json.loads(tag.string)
        except json.JSONDecodeError:
            pass
    return None


def extract_json_ld(html: str) -> list:
    """Return all parsed JSON-LD objects from the page."""
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            obj = json.loads(tag.string or "{}")
            results.append(obj)
        except json.JSONDecodeError:
            pass
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  Search result parsing
# ─────────────────────────────────────────────────────────────────────────────

def parse_search_listings(html: str) -> list:
    """
    Extract ad listing summaries from a search-results page.
    Returns a list of dicts with at minimum: finnkode, url.
    Primary: decode base64 application/json blobs (finn.no TanStack Query hydration).
    Fallback: __NEXT_DATA__, then raw href scanning.
    """
    # Primary: finn.no embeds data in base64 application/json script tags
    blobs = _decode_b64_json_scripts(html)
    docs, _ = _find_docs_and_meta(blobs)
    if docs:
        listings = [_normalise_doc(d) for d in docs if d.get("id") or d.get("finnkode")]
        return [l for l in listings if l["finnkode"]]

    # Fallback 1: __NEXT_DATA__ (older finn.no pages)
    nd = extract_next_data(html)
    if nd:
        listings = _listings_from_next_data(nd)
        if listings:
            return listings

    # Fallback 2: raw href scanning
    return _listings_from_html(html)


def _normalise_doc(doc: dict) -> dict:
    """Normalise a finn.no search-result doc to a uniform dict."""
    finnkode = str(doc.get("id") or doc.get("finnkode") or doc.get("ad_id") or "")
    url = doc.get("canonical_url") or doc.get("url") or ""
    if not url and finnkode:
        # Best-effort URL; detail scraper will follow any redirect
        url = "https://www.finn.no/mobility/item/" + finnkode

    price_raw = doc.get("price", {})
    price = None
    if isinstance(price_raw, dict):
        price = price_raw.get("amount") or price_raw.get("value")
    elif isinstance(price_raw, (int, float)):
        price = int(price_raw)

    return {
        "finnkode":     finnkode,
        "url":          url,
        "title":        doc.get("heading") or doc.get("title", ""),
        "price":        price,
        "year":         doc.get("year"),
        "mileage":      doc.get("mileage"),
        "make":         doc.get("make"),
        "model":        doc.get("model"),
        "location":     doc.get("location"),
        "fuel_type":    doc.get("fuel"),
        "transmission": doc.get("transmission"),
    }


# Keep old name as alias for the detail-page extraction code that calls it.
_normalise_listing = _normalise_doc


def _set_page_param(url: str, page: int) -> str:
    """Return *url* with the page query parameter set to *page*."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["page"] = [str(page)]
    flat = {k: v[0] for k, v in params.items()}
    return urlunparse(parsed._replace(query=urlencode(flat)))


def get_next_page_url(html: str, current_url: str) -> Optional[str]:
    """
    Return the URL of the next search-results page, or None if on the last page.
    Primary: uses the paging metadata in the base64 JSON blobs.
    Fallback: rel="next" link or pagination button in HTML.
    """
    blobs = _decode_b64_json_scripts(html)
    _, meta = _find_docs_and_meta(blobs)
    if isinstance(meta, dict):
        paging = meta.get("paging", {})
        if isinstance(paging, dict):
            current = paging.get("current", 1)
            last    = paging.get("last", 1)
            if current < last:
                return _set_page_param(current_url, current + 1)
            return None  # we are on the last page
        if meta.get("is_end_of_paging"):
            return None

    # Legacy HTML fallback
    soup = BeautifulSoup(html, "html.parser")
    link = soup.find("a", rel=lambda r: r and "next" in r)
    if link and link.get("href"):
        href = link["href"]
        return href if href.startswith("http") else "https://www.finn.no" + href
    btn = soup.find(attrs={"data-testid": "pagination-next"})
    if btn and btn.get("href"):
        href = btn["href"]
        return href if href.startswith("http") else "https://www.finn.no" + href

    return None


def _listings_from_next_data(nd: dict) -> list:
    """Walk the __NEXT_DATA__ tree and collect objects that look like listings."""
    results = []

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            if ("finnkode" in obj or "id" in obj) and ("heading" in obj or "title" in obj):
                results.append(_normalise_doc(obj))
                return
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(nd)
    seen: set = set()
    unique = []
    for item in results:
        fk = item.get("finnkode", "")
        if fk and fk not in seen:
            seen.add(fk)
            unique.append(item)
    return unique


def _listings_from_html(html: str) -> list:
    """Last-resort fallback: extract finn codes from anchor hrefs."""
    soup = BeautifulSoup(html, "html.parser")
    results = []
    seen: set = set()
    pattern = re.compile(r"(?:finnkode=|/item/)(\d+)")
    for a in soup.find_all("a", href=pattern):
        m = pattern.search(a["href"])
        if not m:
            continue
        fk = m.group(1)
        if fk in seen:
            continue
        seen.add(fk)
        href = a["href"]
        url  = href if href.startswith("http") else "https://www.finn.no" + href
        results.append({
            "finnkode": fk,
            "url":      url,
            "title":    a.get_text(strip=True)[:200],
        })
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  Detail page parsing
# ─────────────────────────────────────────────────────────────────────────────

# Map of raw key names (from __NEXT_DATA__ / JSON-LD) → our canonical keys.
FIELD_MAP: dict = {
    "heading":         "title",
    "title":           "title",
    "year":            "year",
    "mileage":         "mileage",
    "kilometrage":     "mileage",
    "fuel":            "fuel_type",
    "fuelType":        "fuel_type",
    "transmission":    "transmission",
    "gearbox":         "transmission",
    "bodyType":        "body_type",
    "body_type":       "body_type",
    "color":           "color",
    "colour":          "color",
    "exteriorColor":   "color",
    "engineEffect":    "engine_power",
    "engine_effect":   "engine_power",
    "horsePower":      "engine_power",
    "driveType":       "drive_type",
    "drive_type":      "drive_type",
    "numDoors":        "num_doors",
    "num_doors":       "num_doors",
    "numberOfDoors":   "num_doors",
    "numSeats":        "num_seats",
    "num_seats":       "num_seats",
    "numberOfSeats":   "num_seats",
    "publishedAt":     "published",
    "published":       "published",
}


def scrape_detail(url: str, finnkode: str) -> Optional[dict]:
    """
    Fetch the ad detail page for *finnkode* and return a dict of all
    extracted fields.  Returns None if the ad is gone or unfetchable.
    """
    resp = fetch(url)
    if resp is None:
        return None

    # If we were redirected to a 404 / not-found page, treat as gone.
    if resp.history and ("not-found" in resp.url or "slettet" in resp.url):
        log.info("Ad %s appears to be gone (redirected to %s)", finnkode, resp.url)
        return None

    data: dict = {
        "_url":        resp.url,
        "_fetched_at": _now(),
        "_finnkode":   finnkode,
    }

    # Primary: base64-encoded application/json blobs (finn.no's current approach)
    for blob in _decode_b64_json_scripts(resp.text):
        if isinstance(blob, dict):
            _merge_known_fields(blob, data)
            # Also deep-walk for nested ad state
            _extract_from_next_data(blob, data)

    # Secondary: classic __NEXT_DATA__
    nd = extract_next_data(resp.text)
    if nd:
        _extract_from_next_data(nd, data)

    # JSON-LD structured data
    for ld in extract_json_ld(resp.text):
        data.setdefault("_json_ld", []).append(ld)
        _extract_from_json_ld(ld, data)

    # Fall back to raw HTML parsing if key fields are still missing.
    if not data.get("title") or not data.get("price"):
        _extract_from_html(resp.text, data)

    return data


# ── __NEXT_DATA__ / generic JSON extraction ───────────────────────────────────

def _extract_from_next_data(nd: dict, out: dict) -> None:
    """Recursively walk __NEXT_DATA__ and populate *out* with ad fields."""

    def walk(obj: Any, depth: int = 0) -> None:
        if depth > 30:
            return
        if isinstance(obj, dict):
            _merge_known_fields(obj, out)
            for v in obj.values():
                walk(v, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                walk(item, depth + 1)

    walk(nd)


def _merge_known_fields(obj: dict, out: dict) -> None:
    """Copy well-known field names from *obj* into *out*."""

    # ── Mapped fields ─────────────────────────────────────────────────────
    for src_key, dst_key in FIELD_MAP.items():
        if src_key not in obj or dst_key in out:
            continue
        val = obj[src_key]
        if dst_key in ("year", "mileage", "num_doors", "num_seats"):
            try:
                val = int(val)
            except (TypeError, ValueError):
                val = None
        if val is not None:
            out[dst_key] = val

    # ── Price ─────────────────────────────────────────────────────────────
    if "price" not in out and "price" in obj:
        raw = obj["price"]
        if isinstance(raw, dict):
            raw = raw.get("amount") or raw.get("value")
        try:
            out["price"] = int(raw)
        except (TypeError, ValueError):
            pass

    # ── Location ─────────────────────────────────────────────────────────
    if "location" not in out and "location" in obj:
        loc = obj["location"]
        if isinstance(loc, dict):
            out["location"] = (
                loc.get("name") or loc.get("city") or loc.get("area") or str(loc)
            )
        elif isinstance(loc, str):
            out["location"] = loc

    # ── Seller ───────────────────────────────────────────────────────────
    if "seller" in obj:
        s = obj["seller"]
        if isinstance(s, dict):
            out.setdefault("seller_name", s.get("name") or s.get("displayName"))
            out.setdefault("seller_type", s.get("type") or s.get("accountType"))

    # ── Images ───────────────────────────────────────────────────────────
    if "num_images" not in out:
        for img_key in ("images", "media", "photos"):
            if img_key in obj and isinstance(obj[img_key], list):
                out["num_images"] = len(obj[img_key])
                break

    # ── Rich text description ─────────────────────────────────────────────
    if "description" not in out and "description" in obj:
        out["description"] = obj["description"]

    # ── Equipment / extras / features list ───────────────────────────────
    for feat_key in ("extras", "equipment", "features", "utstyr"):
        if feat_key not in out and feat_key in obj and isinstance(obj[feat_key], list):
            out[feat_key] = obj[feat_key]

    # ── Attributes array (finn.no uses this extensively) ──────────────────
    if "attributes" in obj and isinstance(obj["attributes"], list):
        existing = out.setdefault("attributes", [])
        for attr in obj["attributes"]:
            if attr not in existing:
                existing.append(attr)
            # Also try to pull individual attribute values into top-level keys
            if isinstance(attr, dict):
                _merge_attribute(attr, out)

    # ── Key-info list (some FINN detail pages use this) ───────────────────
    if "keyInfo" in obj and isinstance(obj["keyInfo"], list):
        out.setdefault("key_info", obj["keyInfo"])

    # ── Finn ad metadata ──────────────────────────────────────────────────
    for meta_key in ("id", "adId", "finnkode", "adType", "category",
                     "subCategory", "status", "registrationClass",
                     "co2Emissions", "registrationDate", "firstRegistration",
                     "technicalInspectionDate", "warrantyInsurance",
                     "importCountry", "modelYear", "trim", "model",
                     "make", "variant"):
        if meta_key in obj:
            out.setdefault(meta_key, obj[meta_key])


def _merge_attribute(attr: dict, out: dict) -> None:
    """
    FINN often uses {key: "...", value: "..."} attribute objects.
    Try to map them to canonical field names.
    """
    key_raw   = str(attr.get("key") or attr.get("label") or attr.get("id") or "").lower()
    value_raw = attr.get("value") or attr.get("rawValue")

    if not key_raw or value_raw is None:
        return

    # Simple heuristic mapping
    attr_to_field: dict = {
        "km":            "mileage",
        "kilometerstand": "mileage",
        "årsmodell":     "year",
        "modellår":      "year",
        "drivstoff":     "fuel_type",
        "girkasse":      "transmission",
        "karosseri":     "body_type",
        "farge":         "color",
        "effekt":        "engine_power",
        "drift":         "drive_type",
        "antall dører":  "num_doors",
        "antall seter":  "num_seats",
        "pris":          "price",
    }

    for pattern, field in attr_to_field.items():
        if pattern in key_raw and field not in out:
            if field in ("mileage", "year", "num_doors", "num_seats"):
                try:
                    clean = re.sub(r"[^\d]", "", str(value_raw))
                    out[field] = int(clean)
                except (ValueError, TypeError):
                    pass
            elif field == "price":
                try:
                    clean = re.sub(r"[^\d]", "", str(value_raw))
                    out[field] = int(clean)
                except (ValueError, TypeError):
                    pass
            else:
                out[field] = str(value_raw)
            break


# ── JSON-LD extraction ────────────────────────────────────────────────────────

def _extract_from_json_ld(ld: dict, out: dict) -> None:
    t = ld.get("@type", "")
    if t not in ("Product", "Car", "Vehicle", "Offer"):
        return

    out.setdefault("title", ld.get("name"))
    out.setdefault("description", ld.get("description"))
    out.setdefault("color", ld.get("color"))
    out.setdefault("fuel_type", ld.get("fuelType"))
    out.setdefault("transmission", ld.get("vehicleTransmission"))
    out.setdefault("drive_type", ld.get("driveWheelConfiguration"))
    out.setdefault("body_type", ld.get("bodyType"))
    out.setdefault("num_doors", ld.get("numberOfDoors"))
    out.setdefault("num_seats", ld.get("vehicleSeatingCapacity"))

    if "mileageFromOdometer" in ld and "mileage" not in out:
        m = ld["mileageFromOdometer"]
        if isinstance(m, dict):
            try:
                out["mileage"] = int(m.get("value", 0))
            except (ValueError, TypeError):
                pass

    if "vehicleModelDate" in ld and "year" not in out:
        try:
            out["year"] = int(str(ld["vehicleModelDate"])[:4])
        except (ValueError, TypeError):
            pass

    if "offers" in ld and "price" not in out:
        offers = ld["offers"]
        if isinstance(offers, dict):
            try:
                out["price"] = int(float(offers.get("price", 0))) or None
            except (TypeError, ValueError):
                pass

    if "brand" in ld:
        b = ld["brand"]
        if isinstance(b, dict):
            out.setdefault("make", b.get("name"))
        elif isinstance(b, str):
            out.setdefault("make", b)

    if "manufacturer" in ld:
        out.setdefault("make", ld["manufacturer"])

    # Seller / dealer
    for seller_key in ("seller", "provider"):
        if seller_key in ld:
            s = ld[seller_key]
            if isinstance(s, dict):
                out.setdefault("seller_name", s.get("name"))


# ── HTML fallback extraction ──────────────────────────────────────────────────

def _extract_from_html(html: str, out: dict) -> None:
    soup = BeautifulSoup(html, "html.parser")

    if not out.get("title"):
        h1 = soup.find("h1")
        if h1:
            out["title"] = h1.get_text(strip=True)

    if not out.get("price"):
        # Look for "123 456 kr" pattern
        price_re = re.compile(r"([\d\s\xa0]+)\s*kr", re.IGNORECASE)
        for tag in soup.find_all(string=price_re):
            m = price_re.search(str(tag))
            if m:
                try:
                    out["price"] = int(re.sub(r"[^\d]", "", m.group(1)))
                    break
                except ValueError:
                    pass

    # Scrape all <dl> key-value pairs as fallback attributes
    for dl in soup.find_all("dl"):
        terms = dl.find_all("dt")
        defs  = dl.find_all("dd")
        for dt, dd in zip(terms, defs):
            key = dt.get_text(strip=True)
            val = dd.get_text(strip=True)
            if key and val:
                out.setdefault(f"_attr_{key}", val)

    # Scrape table rows
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) == 2:
                key = cells[0].get_text(strip=True)
                val = cells[1].get_text(strip=True)
                if key and val:
                    out.setdefault(f"_attr_{key}", val)


# ─────────────────────────────────────────────────────────────────────────────
#  Persistence helpers
# ─────────────────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _data_hash(d: dict) -> str:
    """SHA-256 of the serialised dict with all private (_-prefixed) keys removed."""
    public = {k: v for k, v in d.items() if not k.startswith("_")}
    blob   = json.dumps(public, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode()).hexdigest()


_AD_TYPE_PATTERNS: list = [
    (re.compile(r"/mobility/"),     "car"),
    (re.compile(r"/realestate/"),   "realestate"),
    (re.compile(r"/boat/"),         "boat"),
    (re.compile(r"/job/"),          "job"),
    (re.compile(r"/bap/"),          "bap"),
    (re.compile(r"/mc/"),           "mc"),
    (re.compile(r"/travel/"),       "travel"),
    (re.compile(r"/agriculture/"),  "agriculture"),
]


def infer_ad_type(url: str) -> str:
    """Return a short category string inferred from the finn.no URL path."""
    for pattern, label in _AD_TYPE_PATTERNS:
        if pattern.search(url):
            return label
    return "unknown"


def upsert_ad(db: sqlite3.Connection, finnkode: str, url: str) -> None:
    """Insert a new ad record or ensure an existing one is marked active."""
    ad_type = infer_ad_type(url)
    db.execute(
        """
        INSERT INTO ads (finnkode, url, ad_type, first_seen, last_checked, is_active)
        VALUES (?, ?, ?, ?, ?, 1)
        ON CONFLICT(finnkode) DO UPDATE SET
            is_active    = 1,
            url          = excluded.url,
            ad_type      = excluded.ad_type
        """,
        (finnkode, url, ad_type, _now(), _now()),
    )


def record_snapshot(db: sqlite3.Connection, finnkode: str, detail: dict) -> bool:
    """
    Store a snapshot if the data has changed since the last one.
    Returns True when a new snapshot row was inserted.
    """
    h  = _data_hash(detail)
    ts = _now()

    last = db.execute(
        "SELECT data_hash, raw_data FROM ad_snapshots "
        "WHERE finnkode = ? ORDER BY snapshot_time DESC LIMIT 1",
        (finnkode,),
    ).fetchone()

    if last and last["data_hash"] == h:
        # Nothing changed — just update the heartbeat
        db.execute(
            "UPDATE ads SET last_checked = ? WHERE finnkode = ?",
            (ts, finnkode),
        )
        return False

    raw_json = json.dumps(detail, ensure_ascii=False, default=str)

    db.execute(
        """
        INSERT INTO ad_snapshots (
            finnkode, snapshot_time, data_hash,
            title, price, location,
            seller_name, seller_type,
            published, num_images, description,
            raw_data
        ) VALUES (
            ?, ?, ?,
            ?, ?, ?,
            ?, ?,
            ?, ?, ?,
            ?
        )
        """,
        (
            finnkode, ts, h,
            detail.get("title"),
            detail.get("price"),
            detail.get("location"),
            detail.get("seller_name"),
            detail.get("seller_type"),
            detail.get("published"),
            detail.get("num_images"),
            detail.get("description"),
            raw_json,
        ),
    )

    # Record field-level diffs when this is an *update* (not the first snapshot)
    if last:
        old = json.loads(last["raw_data"])
        _record_field_changes(db, finnkode, ts, old, detail)

    db.execute(
        "UPDATE ads SET last_checked = ? WHERE finnkode = ?",
        (ts, finnkode),
    )
    return True


def _record_field_changes(
    db: sqlite3.Connection,
    finnkode: str,
    changed_at: str,
    old: dict,
    new: dict,
) -> None:
    """Insert one row per changed field into ad_field_changes."""
    all_keys = set(old.keys()) | set(new.keys())
    for key in sorted(all_keys):
        if key.startswith("_"):
            continue  # skip internal / meta keys

        ov = old.get(key)
        nv = new.get(key)

        # Serialise to strings for a stable comparison
        ov_s = json.dumps(ov, sort_keys=True, default=str) if not isinstance(ov, str) else ov
        nv_s = json.dumps(nv, sort_keys=True, default=str) if not isinstance(nv, str) else nv

        if ov_s != nv_s:
            db.execute(
                """
                INSERT INTO ad_field_changes
                    (finnkode, changed_at, field_name, old_value, new_value)
                VALUES (?, ?, ?, ?, ?)
                """,
                (finnkode, changed_at, key, ov_s, nv_s),
            )


# ─────────────────────────────────────────────────────────────────────────────
#  High-level crawl / recheck functions
# ─────────────────────────────────────────────────────────────────────────────

def crawl_search(db: sqlite3.Connection, search_url: str) -> dict:
    """
    Fetch search result pages for *search_url* and upsert every ad found.
    Returns a dict of {finnkode: listing_data} for all ads seen in this run.
    listing_data is the normalised search-result doc and may contain fields
    like year, mileage, make, fuel_type, etc. that are not always available
    on the detail page.
    """
    found: dict = {}
    url: Optional[str] = search_url
    page = 0

    while url and page < MAX_SEARCH_PAGES:
        page += 1
        log.info("Fetching search page %d: %s", page, url)
        resp = fetch(url, pause=(page > 1))
        if resp is None:
            log.warning("Could not fetch search page %d, stopping pagination", page)
            break

        listings = parse_search_listings(resp.text)
        log.info("  %d listings found on page %d", len(listings), page)

        for listing in listings:
            fk   = listing.get("finnkode", "")
            lurl = listing.get("url") or (
                f"https://www.finn.no/mobility/forsale/ad.html?finnkode={fk}"
            )
            if fk:
                found[fk] = listing
                upsert_ad(db, fk, lurl)

        db.commit()

        next_url = get_next_page_url(resp.text, url)
        if next_url and next_url != url:
            url = next_url
        else:
            break  # No further pages

    log.info("Search crawl complete: %d unique ads found", len(found))
    return found


# Fields from search-result listings that are worth patching into snapshots
# when the detail page didn't provide them.
_LISTING_PATCH_FIELDS = ("year", "mileage", "make", "fuel_type", "transmission",
                         "location", "title")


def patch_snapshot_from_listing(
    db: sqlite3.Connection, finnkode: str, listing: dict
) -> bool:
    """
    If the latest snapshot for *finnkode* is missing fields that are present
    in the search-result *listing*, merge them in and save a new snapshot.
    Returns True if a new snapshot was saved.
    """
    last = db.execute(
        "SELECT raw_data FROM ad_snapshots "
        "WHERE finnkode = ? ORDER BY snapshot_time DESC LIMIT 1",
        (finnkode,),
    ).fetchone()
    if not last:
        return False

    existing = json.loads(last["raw_data"])

    # Build a patch: listing fields that are non-None and absent/None in snapshot
    patch = {
        k: listing[k]
        for k in _LISTING_PATCH_FIELDS
        if listing.get(k) is not None and not existing.get(k)
    }
    if not patch:
        return False

    merged = {**existing, **patch}
    log.debug("  Patching %s from listing: %s", finnkode, list(patch.keys()))
    return record_snapshot(db, finnkode, merged)


def process_ads(db: sqlite3.Connection, skip: set) -> None:
    """
    Re-fetch detail pages for all active ads and save snapshots when
    anything has changed.  Ads in *skip* have already been processed
    in this run (initial snapshot) and are skipped to avoid double-fetching.
    """
    rows = db.execute(
        "SELECT finnkode, url FROM ads WHERE is_active = 1 ORDER BY last_checked ASC"
    ).fetchall()

    total    = len(rows)
    updated  = 0
    inactive = 0

    for i, row in enumerate(rows, 1):
        fk  = row["finnkode"]
        url = row["url"]

        if fk in skip:
            continue

        log.info("[%d/%d] Checking %s", i, total, fk)
        detail = scrape_detail(url, fk)

        if detail is None:
            db.execute(
                "UPDATE ads SET is_active = 0 WHERE finnkode = ?", (fk,)
            )
            db.commit()
            log.info("  Marked %s as inactive (ad gone or unreachable)", fk)
            inactive += 1
            continue

        changed = record_snapshot(db, fk, detail)
        db.commit()

        if changed:
            log.info("  Change detected and snapshot saved for %s", fk)
            updated += 1
        else:
            log.debug("  No change for %s", fk)

    log.info(
        "Recheck complete: %d updated, %d marked inactive (of %d total active)",
        updated, inactive, total,
    )
