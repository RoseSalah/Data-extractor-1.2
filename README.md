# Real Estate Listing Optimization

## 📌 Overview
This project is the data crawling section of **Fellowship.AI (Sept 2025)**.  
The goal is to build an intelligent pipeline that scrapes property listings from **Zillow** and **Redfin**, parses structured data, and later runs AI-powered analysis for pricing, content quality, and optimization recommendations.  

The platform aims to help real estate agents identify **mispriced or poorly optimized listings** and provide **actionable improvements** to reduce time-on-market.

---


## 📂 Project Structure
```
REAL-ESTATE-LISTING-OPTIMIZATION/
│── config/
│── data/
│── crawl/
│   ├── __init__.py
│   ├── batch.py
│   ├── extract_search.py
│   ├── fc_extract_adapted.py
│   ├── fetch.py
│   ├── parse_detail.py
│   ├── pipeline.py
│   ├── settings.py
│── .env
│── requirements.txt
│── configs.ipynb
│── debug.ipynb
│── README.md
```

---

## ✅ Current Progress

- **Schema-driven structured data**: Address, price, beds, baths, area, description, images, and more.
- **Batch pipeline**: Fetch → Parse → Run orchestration ([src/pipeline.py](src/pipeline.py)).
- **Raw HTML/JSON storage**: All source files saved for reproducibility.
- **Robust parsing**: Multi-strategy extraction from Redfin/Zillow, with schema.org and regex fallbacks. => now: parsed values return `null` due to mismatch between schema and actual site data.  
- **Configurable areas/zips**: Easily add new cities/zips via [config/listings_config.json](config/listings_config.json).
- **Next step**: implement robust parsing to map real Zillow/Redfin fields to schema.


---

## ⚙️ Usage
Run from the project root:

```bash
# Step 1: Initialize batch and fetch search pages
python -m src.batch

# Step 2: Fetch detail pages
python src/pipeline.py fetch --n 10

# Step 3: Parse details into structured JSON
python src/pipeline.py parse-details --limit 10

# Step 4: End-to-end run (fetch + parse)
python src/pipeline.py run --n 10
```

Outputs are stored in `data/batches/{batch_id}/`:

- `raw/` — raw HTML/JSON snapshots of listing pages
- `structured/` — parsed structured JSON files

---

## 📑 Data Specification

- **Identifiers**: listing_id, platform_id, source_url, batch_id, scraped_timestamp
- **Address**: street, unit, city, state, postal_code
- **Property Attributes**: beds, baths, interior_area_sqft, lot_sqft, year_built, condition
- **Listing Info**: status, list_date, days_on_market, list_price, price_per_sqft
- **Media**: photos, videos, floorplans
- **Description**: textual content
- **Market Signals**: views, saves, share_count
- **Deduplication**: possible_duplicate, duplicate_candidates

See [`config/listings_config.json`](config/listings_config.json) for the full schema.

---
---

## 🚀 Next Steps

- Improve parsing logic for robust extraction of beds, baths, price, etc.
- Integrate Supabase for structured listing storage and media management.
- Expand schema coverage for additional property features and market signals.

---

## 📝 References

- `src/pipeline.py` — main orchestration
- `src/fetch.py` — fetching logic
- `src/parse_detail.py` — parsing logic
- `config/listings_config.json` — schema and area/zips config

---

## 🤝 Contributing

PRs and issues welcome! See `configs.ipynb` for config conventions and data spec.
