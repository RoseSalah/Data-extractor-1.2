# Purpose: Initialize a new batch with ID, folders, and seed search pages.

import json, datetime
from pathlib import Path
from crawl.settings import CFG, make_batch_dirs, today_ymd

def init_batch() -> str:
    """
    Create new batch folders and seed search pages file.
    Returns: BATCH_ID
    """
    # ---- Derive ZIP list from areas ----
    zip_codes = []
    for area in CFG["areas"]:
        for z in area["zips"]:
            zip_codes.append({"city": area["city"], "state": area["state"], "zip": z})

    # ---- Build search pages (per platform per ZIP) ----
    search_pages = []
    for z in zip_codes:
        zip_code = z["zip"]
        # Redfin ZIP search
        search_pages.append({
            "platform_id": "redfin",
            "zip": zip_code,
            "url": CFG["seeds"]["redfin"]["zip_search"].format(ZIP=zip_code)
        })
        # Zillow ZIP search
        search_pages.append({
            "platform_id": "zillow",
            "zip": zip_code,
            "url": CFG["seeds"]["zillow"]["zip_search"].format(ZIP=zip_code)
        })

    # ---- Optional hardcoded detail URLs ----
    detail_pages = [{"platform_id": "unknown", "url": u} for u in CFG["seeds"].get("detail_urls", [])]

    # ---- Create batch_id and dirs ----
    TODAY = today_ymd()
    BATCH_ID = f"{TODAY}_zips{len(zip_codes)}"
    dirs = make_batch_dirs(BATCH_ID)

    # ---- Persist seeds ----
    seeds_path = dirs["structured"] / "seed_search_pages.json"
    seeds_path.write_text(json.dumps({
        "batch_id": BATCH_ID,
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "counts": {
            "zip_total": len(zip_codes),
            "search_pages_total": len(search_pages),
            "detail_pages_total": len(detail_pages)
        },
        "search_pages": search_pages,
        "detail_pages": detail_pages
    }, indent=2), encoding="utf-8")

    print(f"✅ Batch {BATCH_ID} ready at {dirs['base'].resolve()}")
    print(f"Seeds file: {seeds_path}")
    return BATCH_ID
if __name__ == "__main__":
    init_batch()