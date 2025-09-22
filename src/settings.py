# src/settings.py
# Purpose: Centralized settings & helpers (load config, paths, headers, timestamps).

from __future__ import annotations
import json
import datetime
import os
from pathlib import Path
from typing import Dict, Any, Tuple
import sys

# ---------- config loading ----------
def get_project_root() -> Path:
    try:
        # If running in a notebook, get_ipython will exist
        if "get_ipython" in globals():
            return Path(os.getcwd()).parent
        else:
            return Path(__file__).resolve().parent.parent
    except NameError:
        # Fallback if __file__ is not defined
        return Path(os.getcwd()).parent
PROJECT_ROOT = get_project_root()
CONFIG_PATH = PROJECT_ROOT / "config" / "listings_config.json"
def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found at {CONFIG_PATH}")
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

CFG = load_config()

# ---------- runtime knobs ----------
REQUEST_TIMEOUT_SEC: int = int(CFG["run"].get("request_timeout_sec", 30))
SLEEP_RANGE_SEC: Tuple[float, float] = tuple(CFG["run"].get("sleep_range_sec", [1.2, 2.8]))  # (min, max)
USER_AGENT: str = CFG["run"].get("user_agent", "Mozilla/5.0")

# ---------- time helpers ----------
def now_utc_iso() -> str:
    """Return current UTC timestamp as ISO-8601 with Z suffix."""
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def today_ymd() -> str:
    """Return current UTC date as YYYY-MM-DD."""
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")

# ---------- batch folders ----------
def make_batch_dirs(batch_id: str) -> Dict[str, Path]:
    """
    Create and return batch directory paths.
    Returns dict: {'base','raw','structured','qa'}
    """
    base = PROJECT_ROOT / "data" / "batches" / batch_id
    raw = base / "raw"
    structured = base / "structured"
    qa = base / "qa"
    for p in (raw, structured, qa):
        p.mkdir(parents=True, exist_ok=True)
    return {"base": base, "raw": raw, "structured": structured, "qa": qa}

# ---------- HTTP headers ----------
def default_headers() -> Dict[str, str]:
    """
    Build polite default headers for GET requests.
    """
    return {
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
    }