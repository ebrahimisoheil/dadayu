from __future__ import annotations

import csv
from pathlib import Path

UNIVERSE_CSV = Path(__file__).parent.parent.parent / "warehouse" / "seeds" / "index_universe.csv"
INTERVAL_TABLE = {"1d": "index_prices_daily"}
MARKET = "index"


def load_index_universe() -> list[dict]:
    with open(UNIVERSE_CSV, newline="") as f:
        return list(csv.DictReader(f))


def load_symbols() -> list[str]:
    return [row["ticker"] for row in load_index_universe()]
