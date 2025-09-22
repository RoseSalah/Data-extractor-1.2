# Purpose: Orchestrate detail fetching and parsing using the existing modules.

from __future__ import annotations
import argparse
import json
from pathlib import Path
from typing import List, Optional
from crawl.fetch import fetch_detail_pages
from crawl.parse_detail import parse_all_details
from crawl.settings import now_utc_iso

BATCHES_ROOT = Path("data/batches")

def latest_batch() -> Path:
    if not BATCHES_ROOT.exists():
        raise RuntimeError("No batches folder found. Run: python -m src.batch")
    candidates = [p for p in BATCHES_ROOT.iterdir() if p.is_dir()]
    if not candidates:
        raise RuntimeError("No batch directories found. Run: python -m src.batch")
    return max(candidates, key=lambda p: p.stat().st_mtime)

def load_listing_urls(batch_dir: Path) -> List[str]:
    lu_path = batch_dir / "structured" / "listing_urls.json"
    if not lu_path.exists():
        raise FileNotFoundError("structured/listing_urls.json not found. "
                                "Run: python -m src.fetch  then  python -m src.extract_search")
    payload = json.loads(lu_path.read_text(encoding="utf-8"))
    urls = payload.get("urls", [])
    out: List[str] = []
    for row in urls:
        if isinstance(row, dict):
            url = row.get("source_url")
        else:
            url = str(row)
        if url:
            out.append(url)
    if not out:
        raise RuntimeError("No detail URLs inside listing_urls.json")
    return out

def next_detail_index(raw_dir: Path) -> int:
    """Return the next index for detail files (start at 1001)."""
    existing = sorted(raw_dir.glob("1???_raw.html"))
    if not existing:
        return 1001
    last = max(int(p.name[:4]) for p in existing)
    return last + 1

def fetch_details(n: int, batch_id: Optional[str] = None) -> None:
    batch_dir = latest_batch() if batch_id is None else (BATCHES_ROOT / batch_id)
    raw_dir = batch_dir / "raw"
    urls = load_listing_urls(batch_dir)

    start_idx = next_detail_index(raw_dir)
    subset = urls[:n]
    print(f"Batch: {batch_dir.name}")
    print(f"Fetching {len(subset)} details starting at idx {start_idx} ...")
    fetch_detail_pages(subset, batch_id=batch_dir.name, start_idx=start_idx)
    print("✅ fetch-details done at", now_utc_iso())

def parse_details(limit: int, batch_id: Optional[str] = None) -> None:
    batch_dir = latest_batch() if batch_id is None else (BATCHES_ROOT / batch_id)
    print(f"Batch: {batch_dir.name}")
    parse_all_details(batch_id=batch_dir.name, limit=limit)
    print("✅ parse-details done at", now_utc_iso())

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    s1 = sub.add_parser("fetch-details", help="Fetch N detail pages into the latest batch")
    s1.add_argument("--n", type=int, default=10)

    s2 = sub.add_parser("parse-details", help="Parse up to LIMIT detail pages in the latest batch")
    s2.add_argument("--limit", type=int, default=10)

    s3 = sub.add_parser("run", help="Fetch N detail pages then parse them")
    s3.add_argument("--n", type=int, default=10)

    args = ap.parse_args()
    if args.cmd == "fetch-details":
        fetch_details(args.n)
    elif args.cmd == "parse-details":
        parse_details(args.limit)
    elif args.cmd == "run":
        fetch_details(args.n)
        parse_details(args.n)
    else:
        ap.print_help()

if __name__ == "__main__":
    main()
