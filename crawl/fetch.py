# Purpose: Fetch search/detail pages and persist raw HTML + minimal metadata to the batch folders.
from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from crawl.settings import (
    CFG,
    PROJECT_ROOT,
    REQUEST_TIMEOUT_SEC,
    SLEEP_RANGE_SEC,
    default_headers,
    make_batch_dirs,
    now_utc_iso,
)

# ============================ paths & helpers ============================

def _batches_root() -> Path:
    return PROJECT_ROOT / "data" / "batches"

def _find_latest_batch_id() -> Optional[str]:
    """Return latest batch id by folder mtime (or None if none)."""
    root = _batches_root()
    if not root.exists():
        return None
    dirs = [p for p in root.iterdir() if p.is_dir()]
    if not dirs:
        return None
    latest = max(dirs, key=lambda p: p.stat().st_mtime)
    return latest.name

def _resolve_dirs(batch_id: Optional[str]) -> Dict[str, Path]:
    """Create (if needed) and return batch dirs dict using batch_id or latest batch."""
    if batch_id is None:
        batch_id = _find_latest_batch_id()
        if not batch_id:
            raise RuntimeError("No batches found. Run src/batch.py to create one.")
    return make_batch_dirs(batch_id)

def _seeds_path(struct_dir: Path) -> Path:
    return struct_dir / "seed_search_pages.json"

def _detect_platform_from_row(row: Dict[str, str]) -> str:
    """Return 'zillow' | 'redfin' | 'unknown' based on explicit platform_id or URL."""
    p = (row.get("platform_id") or "").lower()
    if p in ("zillow", "redfin"):
        return p
    u = row.get("url", "")
    if "zillow.com" in u:
        return "zillow"
    if "redfin.com" in u:
        return "redfin"
    return "unknown"

def _balanced_mix(rows: List[Dict[str, str]], limit: int) -> List[Dict[str, str]]:
    """
    Return a balanced list (≈50/50) between Zillow & Redfin up to `limit`.
    Falls back to what's available if one side is short.
    """
    z = [r for r in rows if _detect_platform_from_row(r) == "zillow"]
    r = [r for r in rows if _detect_platform_from_row(r) == "redfin"]
    o = [r for r in rows if _detect_platform_from_row(r) == "unknown"]

    random.shuffle(z); random.shuffle(r); random.shuffle(o)

    # target half/half, but never exceed what's available
    take_z = min(max(limit // 2, 1), len(z))
    take_r = min(limit - take_z, len(r))

    mixed: List[Dict[str, str]] = []
    for i in range(max(take_z, take_r)):
        if i < take_z:
            mixed.append(z[i])
        if len(mixed) >= limit:
            break
        if i < take_r:
            mixed.append(r[i])
        if len(mixed) >= limit:
            break

    # fill remaining from unknown, then any leftovers
    remaining = limit - len(mixed)
    if remaining > 0:
        pool = o + z[take_z:] + r[take_r:]
        mixed.extend(pool[:remaining])

    return mixed[:limit]

# ============================ data classes ============================

@dataclass
class FetchResult:
    status: int
    final_url: str
    html_file: str
    meta_file: str
    resp_file: str

# ============================ core fetching ============================

def _infer_platform_id(url: str) -> str:
    host = urlparse(url).hostname or ""
    if "zillow.com" in host:
        return "zillow"
    if "redfin.com" in host:
        return "redfin"
    return "unknown"

def _should_retry(status: int) -> bool:
    """Retry on 429/5xx."""
    return status in (429,) or (500 <= status <= 599)

def fetch_and_save(
    idx: int,
    url: str,
    raw_dir: Path,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = REQUEST_TIMEOUT_SEC,
    max_retries: int = 2,
    seed_kind: str = "search_or_detail",
) -> FetchResult:
    """
    GET one URL and persist:
      - {idx:04d}_raw.html
      - {idx:04d}_meta.json
      - {idx:04d}_response.json
    Adds platform_id automatically to meta.
    Retries with exponential backoff on 429/5xx.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    headers = headers or default_headers()

    attempt = 0
    last_exc: Optional[Exception] = None
    while attempt <= max_retries:
        try:
            r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            status = r.status_code

            html_path = raw_dir / f"{idx:04d}_raw.html"
            meta_path = raw_dir / f"{idx:04d}_meta.json"
            resp_path = raw_dir / f"{idx:04d}_response.json"

            # write HTML (even for non-200 to inspect later)
            html_path.write_text(r.text or "", encoding="utf-8", errors="ignore")

            # response headers snapshot
            resp = {"status": status, "final_url": r.url, "headers": dict(r.headers)}
            resp_path.write_text(json.dumps(resp, indent=2), encoding="utf-8")

            # our minimal meta
            platform_id = _infer_platform_id(r.url or url)
            meta = {
                "requested_url": url,
                "final_url": r.url,
                "status": status,
                "fetched_at": now_utc_iso(),
                "platform_id": platform_id,
                "seed_kind": seed_kind,
                "idx": idx,
            }
            meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

            return FetchResult(status, r.url, str(html_path), str(meta_path), str(resp_path))

        except Exception as e:
            last_exc = e
            status = 0  # network/exception path

        # retry logic
        if attempt < max_retries and (status == 0 or _should_retry(status)):
            backoff = 1.5 ** attempt + random.uniform(0.0, 0.5)
            time.sleep(backoff)
            attempt += 1
            continue
        break

    # if we exit loop without return, raise last exception or a generic error
    if last_exc:
        raise last_exc
    raise RuntimeError(f"Failed to fetch {url}")

def polite_sleep():
    lo, hi = SLEEP_RANGE_SEC
    time.sleep(random.uniform(lo, hi))

# ============================ public entrypoints ============================

def fetch_first_search_page(batch_id: Optional[str] = None) -> FetchResult:
    """
    Load seed_search_pages.json of a batch, fetch the first (prefer Zillow if available),
    and save it as 0001_* files. Returns FetchResult.
    """
    dirs = _resolve_dirs(batch_id)
    struct_dir, raw_dir = dirs["structured"], dirs["raw"]

    seeds = _seeds_path(struct_dir)
    if not seeds.exists():
        raise FileNotFoundError(f"Seeds file not found at {seeds}. Run src/batch.py first.")

    payload = json.loads(seeds.read_text(encoding="utf-8"))
    search_pages: List[Dict[str, str]] = payload.get("search_pages", [])
    if not search_pages:
        raise RuntimeError("No search pages in seeds. Check your config areas/zips.")

    # Prefer a Zillow page to ensure balance; fallback to first available
    first = next((r for r in search_pages if "zillow.com" in r.get("url", "")), None) or search_pages[0]
    url = first["url"]

    # index 1 is reserved for the first search page (0001_* files)
    res = fetch_and_save(1, url, raw_dir, seed_kind="search")
    return res

def fetch_search_pages(batch_id: Optional[str] = None, limit: int = 10) -> List[FetchResult]:
    """
    Fetch multiple search pages from seeds and save as 0001, 0002, ...
    Uses a balanced mix (≈50/50) between Zillow and Redfin when possible.
    Returns list of FetchResult.
    """
    dirs = _resolve_dirs(batch_id)
    struct_dir, raw_dir = dirs["structured"], dirs["raw"]

    seeds = _seeds_path(struct_dir)
    if not seeds.exists():
        raise FileNotFoundError(f"Seeds file not found at {seeds}. Run src/batch.py first.")

    payload = json.loads(seeds.read_text(encoding="utf-8"))
    search_pages: List[Dict[str, str]] = payload.get("search_pages", [])
    if not search_pages:
        raise RuntimeError("No search pages in seeds. Check your config areas/zips.")

    # Prepare a balanced mix
    mixed = _balanced_mix(search_pages, limit)

    results: List[FetchResult] = []
    for i, row in enumerate(mixed, start=1):
        url = row["url"]
        try:
            res = fetch_and_save(i, url, raw_dir, seed_kind="search")
            results.append(res)
            print(f"[{i}/{limit}] {res.status} -> {url}")
        except Exception as e:
            print(f"[{i}/{limit}] ERROR {type(e).__name__}: {e}")
        polite_sleep()
    return results

def fetch_detail_pages(urls: List[str], batch_id: Optional[str] = None, start_idx: int = 1001) -> List[FetchResult]:
    """
    Fetch a list of detail-page URLs and save as 1001, 1002, ...
    Returns list of FetchResult.
    """
    dirs = _resolve_dirs(batch_id)
    raw_dir = dirs["raw"]

    results: List[FetchResult] = []
    for i, url in enumerate(urls, start=0):
        idx = start_idx + i
        try:
            res = fetch_and_save(idx, url, raw_dir, seed_kind="detail")
            results.append(res)
            print(f"[{i+1}] {res.status} -> {url}")
        except Exception as e:
            print(f"[{i+1}] ERROR {type(e).__name__}: {e}")
        polite_sleep()
    return results

# ============================ CLI ============================

if __name__ == "__main__":
    # fetch the first seed search page into the latest batch.
    out = fetch_first_search_page()
    print("✅ Saved:", out)
