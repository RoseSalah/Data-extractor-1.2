# Purpose: Convert saved detail pages (1001_raw.html, 1002_...) 
# into structured JSON using multi-strategy parsing for Redfin & Zillow 
# with schema.org and regex fallbacks

from __future__ import annotations
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
from crawl.settings import (
    make_batch_dirs,
    now_utc_iso,
)

# ---------------------------- helpers ----------------------------

def _latest_batch() -> str:
    root = Path("data/batches")
    latest = max((p for p in root.iterdir() if p.is_dir()), key=lambda p: p.stat().st_mtime, default=None)
    if latest is None:
        raise RuntimeError("No batches found. Run src/batch.py first.")
    return latest.name

def _resolve_dirs(batch_id: Optional[str]) -> Dict[str, Path]:
    return make_batch_dirs(batch_id) if batch_id else make_batch_dirs(_latest_batch())

def safe_float(x) -> Optional[float]:
    """Normalize a numeric-like value to float (strip $ , etc.)."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = re.sub(r"[^\d\.]", "", str(x))
    try:
        return float(s) if s else None
    except Exception:
        return None

def to_int(x) -> Optional[int]:
    v = safe_float(x)
    return int(v) if v is not None else None

def _read_html_meta(raw_dir: Path, idx: int) -> tuple[str, Dict[str, Any]]:
    html = (raw_dir / f"{idx:04d}_raw.html").read_text(encoding="utf-8", errors="ignore")
    meta = json.loads((raw_dir / f"{idx:04d}_meta.json").read_text(encoding="utf-8"))
    return html, meta

# ------------------------- site parsers --------------------------

def parse_redfin(soup: BeautifulSoup, html_text: str) -> Dict[str, Any]:
    """Extract fields from Redfin __NEXT_DATA__ (plus fallbacks)."""
    out = {
        "platform_id": "redfin",
        "external_property_id": None,
        "address": {"street": None, "unit": None, "city": None, "state": None, "postal_code": None},
        "list_price": None,
        "beds": None,
        "baths": None,
        "interior_area_sqft": None,
        "year_built": None,
        "photos": [],
    }

    nxt = soup.find("script", id="__NEXT_DATA__", type="application/json")
    data = {}
    if nxt and nxt.string:
        try:
            data = json.loads(nxt.string)
        except Exception:
            data = {}

    def walk(n):
        if isinstance(n, dict):
            # id
            for k in ("propertyId", "propertyIdStr", "id"):
                v = n.get(k)
                if v and str(v).isdigit():
                    out["external_property_id"] = str(v)
            # address
            if any(k in n for k in ("streetLine", "city", "zip", "postalCode", "state", "stateCode", "unitNumber", "unit")):
                out["address"].update({
                    "street": n.get("streetLine", out["address"]["street"]),
                    "unit": n.get("unitNumber") or n.get("unit") or out["address"]["unit"],
                    "city": n.get("city", out["address"]["city"]),
                    "state": n.get("state") or n.get("stateCode") or out["address"]["state"],
                    "postal_code": str(n.get("zip") or n.get("postalCode") or out["address"]["postal_code"] or "").strip() or None,
                })
            # numerics
            out["list_price"] = out["list_price"] or safe_float(n.get("price") or n.get("listPrice"))
            out["beds"]  = out["beds"]  or safe_float(n.get("beds"))
            out["baths"] = out["baths"] or safe_float(n.get("baths") or n.get("bathsTotal"))
            for kk in ("squareFeet", "sqFt", "livingArea", "livingAreaSqFt", "aboveGradeFinishedArea"):
                if out["interior_area_sqft"] is None and kk in n:
                    out["interior_area_sqft"] = to_int(n.get(kk))
                    break
            yb = n.get("yearBuilt")
            if out["year_built"] is None and (isinstance(yb, (int, float)) or (isinstance(yb, str) and yb.isdigit())):
                out["year_built"] = int(yb)
            # photos
            if "photos" in n and isinstance(n["photos"], list):
                for p in n["photos"]:
                    if isinstance(p, dict):
                        u = p.get("url") or p.get("href") or p.get("src")
                        if u and u not in out["photos"]:
                            out["photos"].append(u)
            for v in n.values():
                walk(v)
        elif isinstance(n, list):
            for v in n:
                walk(v)

    if data:
        walk(data)

    # Regex fallbacks
    if out["interior_area_sqft"] is None:
        m = re.search(r'([\d,\.]+)\s*(sq\s*ft|sqft)', html_text, re.I)
        if m:
            try:
                out["interior_area_sqft"] = int(float(m.group(1).replace(",", "")))
            except Exception:
                pass
    if out["list_price"] is None:
        m = re.search(r'Price[:\s]*\$?\s*([\d,\,\.]+)', html_text, re.I)
        if m:
            try:
                out["list_price"] = float(m.group(1).replace(",", ""))
            except Exception:
                pass

    return out

def parse_zillow(soup: BeautifulSoup, html_text: str) -> Dict[str, Any]:
    """Extract fields from Zillow shared-data/Apollo JSON (plus fallbacks)."""
    out = {
        "platform_id": "zillow",
        "external_property_id": None,
        "address": {"street": None, "unit": None, "city": None, "state": None, "postal_code": None},
        "list_price": None,
        "beds": None,
        "baths": None,
        "interior_area_sqft": None,
        "year_built": None,
        "photos": [],
    }

    payloads: List[Dict[str, Any]] = []
    # data-zrr-shared-data-key blocks
    for sc in soup.find_all("script", attrs={"data-zrr-shared-data-key": True}):
        txt = sc.string or sc.text or ""
        txt = txt.replace("<!--", "").replace("-->", "").strip()
        if not txt:
            continue
        try:
            payloads.append(json.loads(txt))
        except Exception:
            pass
    # hdpApolloPreloadedData
    apollo = soup.find("script", id="hdpApolloPreloadedData", type="application/json")
    if apollo and apollo.string:
        try:
            payloads.append(json.loads(apollo.string))
        except Exception:
            pass

    def walk(n):
        if isinstance(n, dict):
            # zpid
            for k in ("zpid", "zillowId", "propertyId"):
                v = n.get(k)
                if v and str(v).isdigit():
                    out["external_property_id"] = str(v)
            # address
            if any(k in n for k in ("streetAddress", "city", "state", "zipcode", "postalCode", "unitNumber", "unit")):
                out["address"].update({
                    "street": n.get("streetAddress", out["address"]["street"]),
                    "unit": n.get("unitNumber") or n.get("unit") or out["address"]["unit"],
                    "city": n.get("city", out["address"]["city"]),
                    "state": n.get("state", out["address"]["state"]),
                    "postal_code": str(n.get("zipcode") or n.get("postalCode") or out["address"]["postal_code"] or "").strip() or None,
                })
            # numerics
            out["list_price"] = out["list_price"] or safe_float(n.get("price") or n.get("listPrice") or n.get("priceForHDP"))
            out["beds"]  = out["beds"]  or safe_float(n.get("bedrooms") or n.get("beds"))
            out["baths"] = out["baths"] or safe_float(n.get("bathrooms") or n.get("baths"))
            for kk in ("livingArea", "livingAreaValue", "area", "finishedSqFt", "finishedArea"):
                if out["interior_area_sqft"] is None and kk in n:
                    val = safe_float(n.get(kk))
                    if val:
                        out["interior_area_sqft"] = int(val)
                        break
            yb = n.get("yearBuilt")
            if yb and (isinstance(yb, (int, float)) or (isinstance(yb, str) and yb.isdigit())):
                out["year_built"] = int(yb)
            # photos
            for key in ("photos", "media", "photoGallery", "hiResImageLink"):
                v = n.get(key)
                if isinstance(v, list):
                    for p in v:
                        if isinstance(p, dict):
                            u = p.get("url") or p.get("href") or p.get("rawUrl") or p.get("hiRes")
                            if u and u not in out["photos"]:
                                out["photos"].append(u)
                elif isinstance(v, str):
                    if v and v not in out["photos"]:
                        out["photos"].append(v)
            for v in n.values():
                walk(v)
        elif isinstance(n, list):
            for v in n:
                walk(v)

    for pl in payloads:
        walk(pl)

    # Fallbacks
    if out["interior_area_sqft"] is None:
        m = re.search(r'([\d,\.]+)\s*(sq\s*ft|sqft)', html_text, re.I)
        if m:
            try:
                out["interior_area_sqft"] = int(float(m.group(1).replace(",", "")))
            except Exception:
                pass

    return out

def parse_schema_org(soup: BeautifulSoup) -> Dict[str, Any]:
    """Generic schema.org JSON-LD fallback: price, address, beds/baths/sqft, images."""
    out = {
        "external_property_id": None,  # schema.org often lacks explicit listing id
        "address": {"street": None, "unit": None, "city": None, "state": None, "postal_code": None},
        "list_price": None, "beds": None, "baths": None, "interior_area_sqft": None, "year_built": None, "photos": []
    }
    for sc in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(sc.string or "{}")
        except Exception:
            continue

        def walk(n):
            if isinstance(n, dict):
                t = str(n.get("@type") or n.get("type") or "").lower()
                if any(x in t for x in ["residence", "singlefamily", "house", "apartment", "offer", "realestatelisting"]):
                    offer = n.get("offers") or {}
                    if isinstance(offer, dict):
                        out["list_price"] = out["list_price"] or safe_float(offer.get("price") or offer.get("lowPrice") or offer.get("highPrice"))
                    addr = n.get("address") or {}
                    if isinstance(addr, dict):
                        out["address"].update({
                            "street": addr.get("streetAddress", out["address"]["street"]),
                            "city": addr.get("addressLocality", out["address"]["city"]),
                            "state": addr.get("addressRegion", out["address"]["state"]),
                            "postal_code": addr.get("postalCode", out["address"]["postal_code"]),
                        })
                    out["beds"]  = out["beds"]  or safe_float(n.get("numberOfRooms") or n.get("bedrooms"))
                    out["baths"] = out["baths"] or safe_float(n.get("bathroomCount") or n.get("bathrooms"))
                    area = n.get("floorSize") or {}
                    if isinstance(area, dict):
                        out["interior_area_sqft"] = out["interior_area_sqft"] or to_int(area.get("value"))
                    imgs = n.get("image")
                    if isinstance(imgs, list):
                        out["photos"].extend([u for u in imgs if isinstance(u, str)])
                    elif isinstance(imgs, str):
                        out["photos"].append(imgs)
                for v in n.values():
                    walk(v)
            elif isinstance(n, list):
                for v in n:
                    walk(v)
        walk(data)

    # dedupe photos
    out["photos"] = list(dict.fromkeys(out["photos"]))[:50]
    return out

def parse_regex_text(html_text: str) -> Dict[str, Any]:
    """Last-resort regex extraction from raw text."""
    out = {"list_price": None, "beds": None, "baths": None, "interior_area_sqft": None, "year_built": None}
    m = re.search(r'\$[\s]*([\d,]+)', html_text)
    if m:
        try:
            out["list_price"] = float(m.group(1).replace(",", ""))
        except Exception:
            pass
    m = re.search(r'(\d+(?:\.\d+)?)\s*beds?', html_text, re.I)
    if m:
        out["beds"] = safe_float(m.group(1))
    m = re.search(r'(\d+(?:\.\d+)?)\s*baths?', html_text, re.I)
    if m:
        out["baths"] = safe_float(m.group(1))
    m = re.search(r'([\d,\.]+)\s*(sq\s*ft|sqft)', html_text, re.I)
    if m:
        try:
            out["interior_area_sqft"] = int(float(m.group(1).replace(",", "")))
        except Exception:
            pass
    m = re.search(r'year\s*built[:\s]*([12]\d{3})', html_text, re.I)
    if m:
        out["year_built"] = int(m.group(1))
    return out

# --------------------------- core API ---------------------------

@dataclass
class ParsedRecord:
    idx: int
    data: Dict[str, Any]
    path: Path

def parse_one_detail(idx: int, batch_id: Optional[str] = None) -> ParsedRecord:
    """
    Parse one saved detail page (e.g., 1001_raw.html) into structured JSON.
    Returns ParsedRecord with output path.
    """
    dirs = _resolve_dirs(batch_id)
    raw_dir, struct_dir = dirs["raw"], dirs["structured"]

    # read files
    html_text, meta = _read_html_meta(raw_dir, idx)
    source_url = (meta.get("final_url") or meta.get("requested_url") or "").lower()
    soup = BeautifulSoup(html_text, "html.parser")

    # choose site parser
    if "redfin.com" in source_url:
        rec = parse_redfin(soup, html_text)
        platform = "redfin"
    elif "zillow.com" in source_url:
        rec = parse_zillow(soup, html_text)
        platform = "zillow"
    else:
        # try both and pick richer
        a = parse_redfin(soup, html_text); b = parse_zillow(soup, html_text)
        score_a = sum(v is not None for v in [a["list_price"], a["beds"], a["baths"], a["interior_area_sqft"]])
        score_b = sum(v is not None for v in [b["list_price"], b["beds"], b["baths"], b["interior_area_sqft"]])
        rec = a if score_a >= score_b else b
        platform = rec["platform_id"]

    # schema.org fallback
    if not any([rec["list_price"], rec["beds"], rec["baths"], rec["interior_area_sqft"]]):
        srec = parse_schema_org(soup)
        for k in ["list_price", "beds", "baths", "interior_area_sqft", "year_built"]:
            rec[k] = rec[k] if rec[k] is not None else srec.get(k)
        for k in ["street", "unit", "city", "state", "postal_code"]:
            rec["address"][k] = rec["address"].get(k) or srec["address"].get(k)
        rec["photos"] = rec["photos"] or srec["photos"]

    # regex fallback
    if rec["interior_area_sqft"] is None or rec["beds"] is None or rec["baths"] is None or rec["list_price"] is None:
        rrx = parse_regex_text(html_text)
        for k in ["list_price", "beds", "baths", "interior_area_sqft", "year_built"]:
            rec[k] = rec[k] if rec[k] is not None else rrx.get(k)

    # build structured JSON (aligned with config spec core groups)
    structured = {
        "listing_id": None,  # to be assigned later in DB
        "platform_id": platform,
        "source_url": source_url,
        "external_property_id": rec["external_property_id"],
        "batch_id": dirs["base"].name,
        "scraped_timestamp": now_utc_iso(),

        "address": rec["address"],
        "latitude": None,
        "longitude": None,

        "property_type": None,
        "property_subtype": None,
        "beds": rec["beds"],
        "baths": rec["baths"],
        "interior_area_sqft": to_int(rec["interior_area_sqft"]),
        "lot_sqft": None,
        "year_built": to_int(rec["year_built"]),
        "condition": None,

        "listing": {
            "listing_type": "sell",
            "status": None,
            "list_date": None,
            "days_on_market": None,
            "list_price": safe_float(rec["list_price"]),
            "price_per_sqft": None
        },

        "description": None,
        "media": [{"url": u, "type": "image", "caption": None} for u in (rec["photos"] or [])[:50]],
        "features": {},
        "market_signals": {"views": None, "saves": None, "share_count": None},
        "similar_properties": [],
        "possible_duplicate": False,
        "duplicate_candidates": []
    }
    if structured["listing"]["list_price"] and structured["interior_area_sqft"]:
        try:
            structured["listing"]["price_per_sqft"] = round(
                structured["listing"]["list_price"] / structured["interior_area_sqft"], 2
            )
        except Exception:
            structured["listing"]["price_per_sqft"] = None

    out_path = dirs["structured"] / f"{idx:04d}.json"
    out_path.write_text(json.dumps(structured, indent=2), encoding="utf-8")
    print(f"✅ Parsed {idx} -> {out_path}")
    return ParsedRecord(idx=idx, data=structured, path=out_path)

def parse_all_details(batch_id: Optional[str] = None, limit: int = 10, start_idx: int = 1001) -> List[ParsedRecord]:
    """
    Iterate detail files (1001_raw.html, 1002_raw.html, ...) up to `limit`,
    parse to structured JSON, and return a list of ParsedRecord.
    """
    dirs = _resolve_dirs(batch_id)
    raw_dir = dirs["raw"]

    # collect available detail indices (1001_raw.html etc.)
    files = sorted(raw_dir.glob("1???_raw.html"))[:limit]
    if not files:
        raise FileNotFoundError("No detail raw files found (e.g., 1001_raw.html). Run fetch_detail_pages first.")

    results: List[ParsedRecord] = []
    for f in files:
        idx = int(f.name[:4])
        try:
            res = parse_one_detail(idx, batch_id=dirs["base"].name)
            results.append(res)
        except Exception as e:
            print(f"[{idx}] ERROR {type(e).__name__}: {e}")
    return results

# ----------------------------- CLI ------------------------------
if __name__ == "__main__":
    parse_all_details(limit=10)
