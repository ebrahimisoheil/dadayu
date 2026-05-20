from __future__ import annotations

import csv
import json
import re
import time
from pathlib import Path

import pandas as pd
import requests

UNIVERSE_CSV = Path(__file__).parent.parent.parent / "warehouse" / "seeds" / "crypto_universe.csv"
ASSET_MAP_CSV = Path(__file__).parent.parent.parent / "warehouse" / "seeds" / "polymarket_asset_map.csv"
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"


def _load_crypto_tickers() -> list[str]:
    with open(UNIVERSE_CSV, newline="") as f:
        return [row["symbol"].replace("-USD", "") for row in csv.DictReader(f)]


def _load_asset_map() -> dict[str, tuple[str, str]]:
    overrides: dict[str, tuple[str, str]] = {}
    if not ASSET_MAP_CSV.exists():
        return overrides
    with open(ASSET_MAP_CSV, newline="") as f:
        for row in csv.DictReader(f):
            overrides[row["condition_id"]] = (row["linked_asset"], row["asset_type"])
    return overrides


def _parse_linked_asset(question: str, tickers: list[str]) -> tuple[str | None, str | None]:
    question_upper = question.upper()
    for ticker in tickers:
        if len(ticker) < 3:
            continue
        if re.search(rf"\b{re.escape(ticker)}\b", question_upper):
            return f"{ticker}-USD", "crypto"
    return None, None


_CLOB_MAX_WINDOW = 15 * 24 * 3600  # API rejects intervals > 15 days


def _fetch_clob_chunk(
    yes_token_id: str, start_ts: int, end_ts: int, fidelity: int
) -> pd.DataFrame:
    url = f"{CLOB_BASE}/prices-history"
    params = {"market": yes_token_id, "startTs": start_ts, "endTs": end_ts, "fidelity": fidelity}
    for attempt in range(3):
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            wait = min(60, 4 ** attempt)
            print(f"  [WARN] Rate limited — waiting {wait}s (attempt {attempt + 1}/3)...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        history = resp.json().get("history", [])
        if not history:
            return pd.DataFrame(columns=["ts", "probability", "volume_usd"])
        df = pd.DataFrame(history)
        df = df.rename(columns={"t": "ts", "p": "probability", "v": "volume_usd"})
        df["ts"] = pd.to_datetime(df["ts"], unit="s")
        df["probability"] = df["probability"].astype(float)
        if "volume_usd" not in df.columns:
            df["volume_usd"] = 0.0
        else:
            df["volume_usd"] = pd.to_numeric(df["volume_usd"], errors="coerce").fillna(0.0)
        return df[["ts", "probability", "volume_usd"]]
    raise RuntimeError(f"CLOB prices-history failed for {yes_token_id} after 3 attempts")


def _fetch_clob_history(
    yes_token_id: str, start_ts: int, end_ts: int, fidelity: int
) -> pd.DataFrame:
    chunks = []
    chunk_start = start_ts
    while chunk_start < end_ts:
        chunk_end = min(chunk_start + _CLOB_MAX_WINDOW, end_ts)
        chunk = _fetch_clob_chunk(yes_token_id, chunk_start, chunk_end, fidelity)
        if not chunk.empty:
            chunks.append(chunk)
        chunk_start = chunk_end
        if chunk_start < end_ts:
            time.sleep(0.1)
    if not chunks:
        return pd.DataFrame(columns=["ts", "probability", "volume_usd"])
    return pd.concat(chunks, ignore_index=True).drop_duplicates(subset=["ts"])


def discover_markets(min_volume_usd: float = 50_000) -> pd.DataFrame:
    url = f"{GAMMA_BASE}/markets"
    params = {
        "active": "true",
        "closed": "false",
        "volume_num_min": min_volume_usd,
        "limit": 500,
    }
    for attempt in range(3):
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            wait = min(60, 4 ** attempt)
            print(f"  [WARN] Gamma API rate limited — waiting {wait}s (attempt {attempt + 1}/3)...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        data = resp.json()
        break
    else:
        raise RuntimeError("Gamma API /markets failed after 3 attempts")

    tickers = _load_crypto_tickers()
    overrides = _load_asset_map()
    rows = []
    for m in data:
        condition_id = m.get("conditionId", "")
        clob_ids_raw = m.get("clobTokenIds", "[]")
        try:
            clob_ids = json.loads(clob_ids_raw) if isinstance(clob_ids_raw, str) else clob_ids_raw
            yes_token_id = clob_ids[0] if clob_ids else ""
        except (json.JSONDecodeError, IndexError):
            yes_token_id = ""
        linked_asset, asset_type = _parse_linked_asset(m.get("question", ""), tickers)
        if condition_id in overrides:
            linked_asset, asset_type = overrides[condition_id]

        resolution_raw = m.get("endDate")
        resolution_date = pd.Timestamp(resolution_raw) if resolution_raw else None

        rows.append({
            "condition_id": condition_id,
            "question": m.get("question", ""),
            "category": m.get("category", ""),
            "volume_usd": float(m.get("volume", 0) or 0),
            "liquidity_usd": float(m.get("liquidity", 0) or 0),
            "active": bool(m.get("active", False)),
            "closed": bool(m.get("closed", False)),
            "resolution_date": resolution_date,
            "outcome": m.get("outcome"),
            "yes_token_id": yes_token_id,
            "linked_asset": linked_asset,
            "asset_type": asset_type,
        })

    if not rows:
        return pd.DataFrame(columns=[
            "condition_id", "question", "category", "volume_usd", "liquidity_usd",
            "active", "closed", "resolution_date", "outcome", "yes_token_id",
            "linked_asset", "asset_type",
        ])
    return pd.DataFrame(rows)


def fetch_price_history(yes_token_id: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    return _fetch_clob_history(yes_token_id, start_ts, end_ts, fidelity=60)


def fetch_daily_price_history(yes_token_id: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    return _fetch_clob_history(yes_token_id, start_ts, end_ts, fidelity=1440)
