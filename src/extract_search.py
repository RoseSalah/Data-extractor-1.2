#Purpose: Parse saved search pages (0001_raw.html, 0002_...) 
# to extract listing detail URLs for Redfin & Zillow; dedupe and persist to structured/listing_urls.json

from __future__ import annotations
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from src.settings import make_batch_dirs

RED_FIN = re.compile(r"^https?://(?:www\.)?redfin\.com/.+/home/(\d+)", re.I)
ZILL_OW = re.compile(r"^https?://(?:www\.)?zillow\.com/homedetails/.+?(\d+)_zpid/?", re.I)

def _resolve_dirs(batch_id: str | None) -> Dict[str, Path]:
    return make_batch_dirs(batch_id) if batch_id else make_batch_dirs(_latest_batch())

def _latest_batch() -> str:
    root = Path("data/batches")
    latest = max((p for p in root.iterdir() if p.is_dir()), key=lambda p: p.stat().st_mtime, default=None)
    if latest is None:
        raise RuntimeError("No batches found. Run src/batch.py first.")
    return latest.name
def _collect_from_html(html_path: Path, base_hint: str | None = None) -> List[str]:
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")
    links = set()

    # 1) anchors
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("/"):
            # infer base by hint (file metadata) or try both site bases
            if base_hint and "redfin.com" in base_hint:
                href = urljoin("https://www.redfin.com/", href)
            elif base_hint and "zillow.com" in base_hint:
                href = urljoin("https://www.zillow.com/", href)
        links.add(href)

    # 2) Redfin: __NEXT_DATA__ JSON (richer)
    nxt = soup.find("script", id="__NEXT_DATA__", type="application/json")
    if nxt and nxt.string:
        try:
            data = json.loads(nxt.string)
            def walk(node):
                if isinstance(node, dict):
                    u = node.get("url")
                    if isinstance(u, str):
                        if u.startswith("/"):
                            u = urljoin("https://www.redfin.com", u)
                        links.add(u)
                    for v in node.values(): walk(v)
                elif isinstance(node, list):
                    for v in node: walk(v)
            walk(data)
        except Exception:
            pass

    return list(links)
def extract_listing_urls(batch_id: str | None = None, max_search_files: int = 4) -> Path:
    dirs = _resolve_dirs(batch_id)
    raw_dir, struct_dir = dirs["raw"], dirs["structured"]

    # find available search files (0001_raw.html, 0002_...)
    files = sorted(raw_dir.glob("0???_raw.html"))[:max_search_files]
    if not files:
        raise FileNotFoundError("No search raw files found (e.g., 0001_raw.html). Run src/fetch.py first.")

    # try to read base hints from meta files
    base_hints: Dict[str, str] = {}
    for f in files:
        m = (raw_dir / f.name.replace("_raw.html", "_meta.json"))
        if m.exists():
            try:
                meta = json.loads(m.read_text(encoding="utf-8"))
                base_hints[f.name] = meta.get("final_url") or meta.get("requested_url") or ""
            except Exception:
                base_hints[f.name] = ""

    # gather links from all files
    all_links: List[str] = []
    for f in files:
        base_hint = base_hints.get(f.name)
        all_links.extend(_collect_from_html(f, base_hint))

    # filter to detail pages + dedupe by (platform, id)
    rows, seen = [], set()
    for href in all_links:
        m_r = RED_FIN.search(href)
        m_z = ZILL_OW.search(href)
        if m_r:
            ext_id = m_r.group(1)
            key = ("redfin", ext_id)
            if key in seen: 
                continue
            seen.add(key)
            rows.append({
                "platform_id": "redfin",
                "source_url": href,
                "external_property_id": ext_id
            })
        elif m_z:
            ext_id = m_z.group(1)
            key = ("zillow", ext_id)
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "platform_id": "zillow",
                "source_url": href,
                "external_property_id": ext_id
            })

    out_path = struct_dir / "listing_urls.json"
    out_path.write_text(json.dumps({
        "count": len(rows),
        "urls": rows
    }, indent=2), encoding="utf-8")

    print(f"✅ Extracted {len(rows)} listing URLs -> {out_path}")
    return out_path

if __name__ == "__main__":
    extract_listing_urls()