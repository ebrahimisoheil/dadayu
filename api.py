#!/usr/bin/env python3
"""FastAPI — trigger hourly price fetching jobs."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException

app = FastAPI(title="DADAYU Hourly Price API")

HERE = Path(__file__).parent
VALID_MARKETS = {"germany", "us", "india", "all"}


def _run(args: list[str]) -> dict:
    cmd = [sys.executable, str(HERE / "fetch_hourly_prices.py")] + args
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(HERE))
    return {
        "returncode": result.returncode,
        "stdout": result.stdout[-4000:] if result.stdout else "",
        "stderr": result.stderr[-2000:] if result.stderr else "",
    }


@app.get("/health")
def health():
    return {"status": "ok"}


VALID_INTERVALS = {"1h", "4h", "1d"}


@app.post("/run/fetch-prices")
def fetch_prices(
    market: str = "all",
    interval: str = "1h",
    start: str | None = None,
    end: str | None = None,
):
    """Fetch OHLCV and store in ClickHouse.

    - market: germany | us | india | all (default: all)
    - interval: 1h | 4h | 1d (default: 1h)
    - start/end: YYYY-MM-DD (default: last calendar month)
    """
    if market not in VALID_MARKETS:
        raise HTTPException(status_code=400, detail=f"market must be one of {VALID_MARKETS}")
    if interval not in VALID_INTERVALS:
        raise HTTPException(status_code=400, detail=f"interval must be one of {VALID_INTERVALS}")

    args = ["--market", market, "--interval", interval]
    if start:
        args += ["--start", start]
    if end:
        args += ["--end", end]

    result = _run(args)
    if result["returncode"] != 0:
        raise HTTPException(status_code=500, detail=result)
    return result


@app.post("/run/fetch-ticker-info")
def fetch_ticker_info(market: str = "all"):
    """Fetch yfinance metadata and append to ClickHouse tickers table.

    - market: germany | us | india | all (default: all)
    """
    if market not in VALID_MARKETS:
        raise HTTPException(status_code=400, detail=f"market must be one of {VALID_MARKETS}")

    cmd = [sys.executable, str(HERE / "fetch_ticker_info.py"), "--market", market]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(HERE))
    out = {
        "returncode": result.returncode,
        "stdout": result.stdout[-4000:] if result.stdout else "",
        "stderr": result.stderr[-2000:] if result.stderr else "",
    }
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=out)
    return out


@app.post("/run/fetch-crypto-prices")
def fetch_crypto_prices(
    interval: str = "1d",
    from_date: str | None = None,
    to_date: str | None = None,
):
    """Fetch crypto OHLCV for top-20 assets → ClickHouse.

    - interval: 1h | 4h | 1d (default: 1d)
    - from_date / to_date: YYYY-MM-DD (default: last calendar month)
    """
    if interval not in VALID_INTERVALS:
        raise HTTPException(status_code=400, detail=f"interval must be one of {VALID_INTERVALS}")

    args = ["--interval", interval]
    if from_date:
        args += ["--start", from_date]
    if to_date:
        args += ["--end", to_date]

    cmd = [sys.executable, str(HERE / "fetch_crypto_prices.py")] + args
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(HERE))
    out = {
        "returncode": result.returncode,
        "stdout": result.stdout[-4000:] if result.stdout else "",
        "stderr": result.stderr[-2000:] if result.stderr else "",
    }
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=out)
    return out


@app.post("/run/fetch-crypto-info")
def fetch_crypto_info():
    """Fetch CoinGecko metadata → ClickHouse crypto_metadata."""
    cmd = [sys.executable, str(HERE / "fetch_crypto_info.py")]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(HERE))
    out = {
        "returncode": result.returncode,
        "stdout": result.stdout[-4000:] if result.stdout else "",
        "stderr": result.stderr[-2000:] if result.stderr else "",
    }
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=out)
    return out
