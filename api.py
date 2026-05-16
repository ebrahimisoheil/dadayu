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
