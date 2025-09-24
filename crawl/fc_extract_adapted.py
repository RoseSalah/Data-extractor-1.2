# src/fc_extract_adapted.py
# Purpose: Firecrawl-based extractor that emits JSON (arrays) for all schema tables:
# listings, properties, media, agents, price_history, locations, engagement, similar_properties

from __future__ import annotations
import os, json, re, time, hashlib
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from pydantic import BaseModel, Field
from firecrawl import FirecrawlApp
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
load_dotenv()
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY", "").strip()
BATCHES_ROOT = Path("data/batches")

def latest_batch_dir() -> Path:
    if not BATCHES_ROOT.exists():
        raise RuntimeError("data/batches not found. Run your batch/fetch steps first.")
    candidates = [p for p in BATCHES_ROOT.iterdir() if p.is_dir()]
    if not candidates:
        raise RuntimeError("No batch folder found.")
    return max(candidates, key=lambda p: p.stat().st_mtime)

# -----------------------------------------------------------------------------
# Pydantic Schemas (new DB-ish rows)
# -----------------------------------------------------------------------------
class Address(BaseModel):
    street: Optional[str] = None
    unit: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

class PropertyRow(BaseModel):
    property_id: str
    street_address: Optional[str] = None
    unit_number: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    interior_area_sqft: Optional[int] = None
    lot_size_sqft: Optional[int] = None
    year_built: Optional[int] = None
    beds: Optional[float] = None
    baths: Optional[float] = None
    property_type: Optional[str] = None
    property_subtype: Optional[str] = None
    condition: Optional[str] = None
    features: Optional[Dict[str, Any]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

class ListingRow(BaseModel):
    listing_id: str
    property_id: str
    batch_id: Optional[str] = None
    platform_id: Optional[str] = None
    source_url: str
    scraped_timestamp: Optional[str] = None
    list_date: Optional[str] = None
    days_on_market: Optional[int] = None
    description: Optional[str] = None
    listing_type: Optional[str] = None
    status: Optional[str] = None
    title: Optional[str] = None

class MediaRow(BaseModel):
    listing_id: str
    media_url: str
    caption: Optional[str] = None
    display_order: int = 0
    is_primary: bool = False
    created_at: Optional[str] = None
    media_type: Optional[str] = "image"

class AgentRow(BaseModel):
    listing_id: str
    agent_name: Optional[str] = None
    phone: Optional[str] = None
    brokerage: Optional[str] = None
    email: Optional[str] = None

class PriceHistoryRow(BaseModel):
    listing_id: str
    event_date: Optional[str] = None     # ISO date if possible
    event_type: Optional[str] = None     # e.g., "listed", "price_change", "sold", "pending"
    price: Optional[int] = None
    notes: Optional[str] = None

class LocationRow(BaseModel):
    location_id: str
    street_address: Optional[str] = None
    unit_number: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

class EngagementRow(BaseModel):
    listing_id: str
    views: Optional[int] = None
    saves: Optional[int] = None
    shares: Optional[int] = None

class SimilarRow(BaseModel):
    listing_id: str
    similar_url: str

# -----------------------------------------------------------------------------
# What we ask Firecrawl to extract from a *detail page*
# -----------------------------------------------------------------------------
class ExtractedAgent(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    brokerage: Optional[str] = None
    email: Optional[str] = None

class ExtractedPriceEvent(BaseModel):
    event_date: Optional[str] = None
    event_type: Optional[str] = None
    price: Optional[str] = None
    notes: Optional[str] = None

class ExtractedDetail(BaseModel):
    platform_id: Optional[str] = Field(None, description="e.g., redfin, zillow, realtor")
    source_url: str
    external_property_id: Optional[str] = None
    scraped_timestamp: Optional[str] = None

    address: Address
    list_price: Optional[str] = None
    listing_type: Optional[str] = None
    status: Optional[str] = None
    list_date: Optional[str] = None
    days_on_market: Optional[str] = None

    beds: Optional[str] = None
    baths: Optional[str] = None
    interior_area_sqft: Optional[str] = None
    lot_size_sqft: Optional[str] = None
    year_built: Optional[str] = None
    property_type: Optional[str] = None
    property_subtype: Optional[str] = None
    condition: Optional[str] = None

    description: Optional[str] = None
    features: Optional[Dict[str, Any]] = None

    images: Optional[List[str]] = None
    agents: Optional[List[ExtractedAgent]] = None
    price_history: Optional[List[ExtractedPriceEvent]] = None

    # extra
    hoa_fee: Optional[str] = None
    property_taxes_annual: Optional[str] = None
    metrics_views: Optional[str] = None
    metrics_saves: Optional[str] = None
    metrics_shares: Optional[str] = None
    similar_properties: Optional[List[str]] = None

class ExtractedDetailPage(BaseModel):
    details: ExtractedDetail

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def stable_uuid(*parts: str) -> str:
    s = "|".join([p for p in parts if p])
    return hashlib.sha1(s.encode("utf-8")).hexdigest()  # 40 hex chars

def to_int(x) -> Optional[int]:
    if x is None: return None
    s = re.sub(r"[^\d\.]", "", str(x))
    if not s: return None
    try: return int(float(s))
    except: return None

def to_float(x) -> Optional[float]:
    if x is None: return None
    s = re.sub(r"[^\d\.]", "", str(x))
    if not s: return None
    try: return float(s)
    except: return None

def make_location_id(addr: Address) -> str:
    # deterministic id from address + lat/long if present
    key = "|".join([
        addr.street or "", addr.unit or "", addr.city or "", addr.state or "",
        addr.postal_code or "", str(addr.latitude or ""), str(addr.longitude or "")
    ])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()

def normalize_detail(d: ExtractedDetail, batch_id: str) -> Dict[str, List[Dict[str, Any]]]:
    platform = (d.platform_id or "").lower().strip() or "unknown"
    ext = (d.external_property_id or "").strip()
    url = d.source_url.strip()
    listing_id = stable_uuid(platform, ext or url)
    property_id = listing_id

    # listings
    L = ListingRow(
        listing_id=listing_id,
        property_id=property_id,
        batch_id=batch_id,
        platform_id=platform,
        source_url=url,
        scraped_timestamp=d.scraped_timestamp,
        list_date=d.list_date,
        days_on_market=to_int(d.days_on_market),
        description=d.description,
        listing_type=(d.listing_type or "sell"),
        status=d.status,
        title=None,
    ).model_dump()

    # properties
    addr = d.address or Address()
    P = PropertyRow(
        property_id=property_id,
        street_address=addr.street,
        unit_number=addr.unit,
        city=addr.city, state=addr.state, postal_code=addr.postal_code,
        latitude=addr.latitude, longitude=addr.longitude,
        interior_area_sqft=to_int(d.interior_area_sqft),
        lot_size_sqft=to_int(d.lot_size_sqft),
        year_built=to_int(d.year_built),
        beds=to_float(d.beds),
        baths=to_float(d.baths),
        property_type=d.property_type,
        property_subtype=d.property_subtype,
        condition=d.condition,
        features=(d.features or {}),
        created_at=d.scraped_timestamp,
        updated_at=d.scraped_timestamp,
    ).model_dump()

    # media
    media_rows: List[Dict[str, Any]] = []
    for i, u in enumerate((d.images or [])[:50]):
        media_rows.append(
            MediaRow(
                listing_id=listing_id,
                media_url=u,
                caption=None,
                display_order=i,
                is_primary=(i == 0),
                created_at=d.scraped_timestamp,
                media_type="image"
            ).model_dump()
        )

    # agents
    agent_rows: List[Dict[str, Any]] = []
    for ag in (d.agents or []):
        agent_rows.append(AgentRow(
            listing_id=listing_id,
            agent_name=(ag.name or None),
            phone=(ag.phone or None),
            brokerage=(ag.brokerage or None),
            email=(ag.email or None),
        ).model_dump())

    # price_history
    ph_rows: List[Dict[str, Any]] = []
    for ev in (d.price_history or []):
        ph_rows.append(PriceHistoryRow(
            listing_id=listing_id,
            event_date=ev.event_date,
            event_type=ev.event_type,
            price=to_int(ev.price),
            notes=ev.notes,
        ).model_dump())

    # locations (dedicated)
    location_id = make_location_id(addr)
    loc_row = LocationRow(
        location_id=location_id,
        street_address=addr.street,
        unit_number=addr.unit,
        city=addr.city, state=addr.state, postal_code=addr.postal_code,
        latitude=addr.latitude, longitude=addr.longitude,
    ).model_dump()

    # engagement
    eng_row = EngagementRow(
        listing_id=listing_id,
        views=to_int(d.metrics_views),
        saves=to_int(d.metrics_saves),
        shares=to_int(d.metrics_shares),
    ).model_dump()

    # similar properties
    sim_rows: List[Dict[str, Any]] = []
    for su in (d.similar_properties or []):
        if su: sim_rows.append(SimilarRow(listing_id=listing_id, similar_url=su).model_dump())

    return {
        "listings": [L],
        "properties": [P],
        "media": media_rows,
        "agents": agent_rows,
        "price_history": ph_rows,
        "locations": [loc_row],
        "engagement": [eng_row],
        "similar_properties": sim_rows,
    }

def dump_json(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

# -----------------------------------------------------------------------------
# Firecrawl prompt & call
# -----------------------------------------------------------------------------
PROMPT = """
You are extracting a SINGLE property detail page into this JSON schema (all fields optional except source_url/address).

Return:
- details: {
  platform_id, source_url, external_property_id, scraped_timestamp,
  address: { street, unit, city, state, postal_code, latitude, longitude },
  list_price, listing_type, status, list_date, days_on_market,
  beds, baths, interior_area_sqft, lot_size_sqft, year_built,
  property_type, property_subtype, condition,
  description, features (object/map),
  images [array of direct image URLs],
  agents [ { name, phone, brokerage, email } ],
  price_history [ { event_date, event_type, price, notes } ],
  hoa_fee, property_taxes_annual,
  metrics_views, metrics_saves, metrics_shares,
  similar_properties [array of URLs]
}

Rules:
- Parse numbers/dates/ids from the page if visible. Do NOT invent.
- Address: fill granular parts if present.
- images: prefer high-res URLs (avoid thumbnails), limit 50.
- price_history: include any table of events (list, price changes, pending, sold).
- metrics_*: parse visible counts (views/saves/shares) if present.
- similar_properties: collect visible "similar/nearby" property URLs.

Return exactly one object in "details".
"""

def extract_one(fc: FirecrawlApp, url: str) -> Optional[ExtractedDetail]:
    try:
        result = fc.extract(
            [url],
            prompt=PROMPT,
            schema=ExtractedDetailPage.model_json_schema()
        )
        # Normalize Firecrawl result (dict or object)
        if isinstance(result, dict):
            data = result.get("data") or result
        else:
            data = getattr(result, "data", None) or {}
        details = data.get("details") or data.get("0", {}).get("details")
        if not details:
            items = data.get("items") or []
            if items and isinstance(items[0], dict):
                details = items[0].get("details")
        if not details:
            return None
        return ExtractedDetail.model_validate(details)
    except Exception as e:
        print("extract_one error:", type(e).__name__, e)
        return None

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main(batch_id: Optional[str] = None, limit: int = 10, delay_sec: float = 1.0):
    if not FIRECRAWL_API_KEY:
        raise RuntimeError("Set FIRECRAWL_API_KEY in your environment (.env).")
    fc = FirecrawlApp(api_key=FIRECRAWL_API_KEY)

    batch_dir = latest_batch_dir() if not batch_id else (BATCHES_ROOT / batch_id)
    struct_dir = batch_dir / "structured"
    urls_path = struct_dir / "listing_urls.json"
    if not urls_path.exists():
        raise FileNotFoundError(f"{urls_path} not found. Run extract_search first.")
    payload = json.loads(urls_path.read_text(encoding="utf-8"))
    url_rows = payload.get("urls") or []
    urls = [r["source_url"] if isinstance(r, dict) else str(r) for r in url_rows][:limit]
    if not urls:
        raise RuntimeError("No URLs to extract.")

    print(f"Batch: {batch_dir.name} | URLs: {len(urls)}")

    listings: List[Dict[str, Any]] = []
    properties: List[Dict[str, Any]] = []
    media: List[Dict[str, Any]] = []
    agents: List[Dict[str, Any]] = []
    price_history: List[Dict[str, Any]] = []
    locations: Dict[str, Dict[str, Any]] = {}  # de-duplicate by location_id
    engagement: List[Dict[str, Any]] = []
    similar_properties: List[Dict[str, Any]] = []

    for i, url in enumerate(urls, 1):
        print(f"[{i}/{len(urls)}] {url}")
        det = extract_one(fc, url)
        if not det:
            print("   → no details extracted")
            time.sleep(delay_sec); continue

        det.source_url = det.source_url or url
        rows = normalize_detail(det, batch_id=batch_dir.name)

        listings.extend(rows["listings"])
        properties.extend(rows["properties"])
        media.extend(rows["media"])
        agents.extend(rows["agents"])
        price_history.extend(rows["price_history"])
        engagement.extend(rows["engagement"])

        # locations: dedupe by location_id
        for loc in rows["locations"]:
            lid = loc["location_id"]
            locations[lid] = loc

        # similar
        similar_properties.extend(rows["similar_properties"])

        time.sleep(delay_sec)

    # Write JSON arrays
    dump_json(struct_dir / "listings.json", listings)
    dump_json(struct_dir / "properties.json", properties)
    dump_json(struct_dir / "media.json", media)
    dump_json(struct_dir / "agents.json", agents)
    dump_json(struct_dir / "price_history.json", price_history)
    dump_json(struct_dir / "locations.json", list(locations.values()))
    dump_json(struct_dir / "engagement.json", engagement)
    dump_json(struct_dir / "similar_properties.json", similar_properties)

    print(f"✅ Wrote JSON files to {struct_dir}")
    print(f"   listings={len(listings)}, properties={len(properties)}, media={len(media)}, agents={len(agents)},")
    print(f"   price_history={len(price_history)}, locations={len(locations)}, engagement={len(engagement)}, similar={len(similar_properties)}")

if __name__ == "__main__":
    # Run:  python -m src.fc_extract_adapted
    main(batch_id=None, limit=10, delay_sec=1.0)
