# Polymarket Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest Polymarket prediction market metadata and hourly probability time series into ClickHouse, model probability signals in dbt, enabling correlation analysis between prediction markets and crypto/equity prices.

**Architecture:** Two Dagster assets (`polymarket_markets` + `polymarket_prices`) write to two new ClickHouse tables. dbt transforms raw data through staging → intermediate (1h/1d resampling) → mart (`fct_polymarket_signals` with Δp, log_odds, is_interpolated, days_to_resolution). A Dagster job runs on the same 4-hour cadence as crypto. Per-market watermarking ensures only new price data is fetched on each run.

**Tech Stack:** Python requests, pandas, clickhouse_connect, Dagster 1.7, dbt-clickhouse, ClickHouse ReplacingMergeTree, Polymarket Gamma API + CLOB API (both public, no auth)

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Modify | `db/clickhouse_init.sql` | Add `polymarket_markets` and `polymarket_prices` tables |
| Modify | `dadayu/watermark.py` | Add `condition_id` parameter for per-market watermarking |
| Modify | `tests/test_watermark.py` | Tests for condition_id watermark behavior |
| Create | `dadayu/ingest/polymarket.py` | `discover_markets`, `fetch_price_history`, `fetch_daily_price_history` |
| Create | `tests/test_polymarket_ingest.py` | Unit tests for ingest functions |
| Create | `dagster_pipeline/assets/polymarket.py` | `polymarket_markets`, `polymarket_prices` Dagster assets |
| Modify | `dagster_pipeline/schedules.py` | Add `polymarket_job` + `polymarket_schedule` |
| Modify | `dagster_pipeline/definitions.py` | Register polymarket assets, job, schedule |
| Create | `warehouse/seeds/polymarket_asset_map.csv` | Manual overrides: condition_id → linked_asset |
| Create | `warehouse/models/staging/polymarket/_sources.yml` | dbt source declarations |
| Create | `warehouse/models/staging/polymarket/stg_polymarket__markets.sql` | Cast + clean market metadata |
| Create | `warehouse/models/staging/polymarket/stg_polymarket__markets.yml` | Column docs |
| Create | `warehouse/models/staging/polymarket/stg_polymarket__prices.sql` | Filter + cast price snapshots |
| Create | `warehouse/models/staging/polymarket/stg_polymarket__prices.yml` | Column docs |
| Create | `warehouse/models/intermediate/int_polymarket_prices_1h.sql` | Resample to hourly buckets |
| Create | `warehouse/models/intermediate/int_polymarket_prices_1d.sql` | Resample to daily buckets |
| Create | `warehouse/models/marts/markets/fct_polymarket_signals.sql` | Probability signals mart |
| Create | `warehouse/models/marts/markets/fct_polymarket_signals.yml` | Column docs |
| Create | `warehouse/snapshots/snap_dim_polymarket_market.sql` | SCD Type 2 on market metadata |

---

### Task 1: ClickHouse Schema

Add two new tables to `db/clickhouse_init.sql`. The running container won't auto-apply (init.sql only runs on first boot) — apply manually after editing.

**Files:**
- Modify: `db/clickhouse_init.sql`

- [ ] **Step 1: Append to `db/clickhouse_init.sql`**

Add to the end of the file:

```sql
-- Polymarket market metadata — upserted daily via Gamma API
CREATE TABLE IF NOT EXISTS polymarket_markets
(
    condition_id      String,
    question          String,
    category          String,
    volume_usd        Float64,
    liquidity_usd     Float64,
    active            Bool,
    closed            Bool,
    resolution_date   Nullable(DateTime),
    outcome           Nullable(String),
    yes_token_id      String,
    linked_asset      Nullable(String),
    asset_type        Nullable(String),
    fetched_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY condition_id;


-- Polymarket hourly probability snapshots — watermarked per condition_id
CREATE TABLE IF NOT EXISTS polymarket_prices
(
    condition_id  String,
    ts            DateTime,
    probability   Float64,
    volume_usd    Float64,
    ingested_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (condition_id, ts)
PARTITION BY toYYYYMM(ts);
```

- [ ] **Step 2: Apply to the running container**

```bash
docker exec -i dadayu_clickhouse clickhouse-client \
  --user dadayu --password changeme --database dadayu \
  < db/clickhouse_init.sql
```

Expected: no output (all DDL is `IF NOT EXISTS` — idempotent).

- [ ] **Step 3: Verify tables exist**

```bash
docker exec dadayu_clickhouse clickhouse-client \
  --user dadayu --password changeme --database dadayu \
  --query "SHOW TABLES LIKE 'polymarket%'"
```

Expected:
```
polymarket_markets
polymarket_prices
```

- [ ] **Step 4: Commit**

```bash
git add db/clickhouse_init.sql
git commit -m "feat(clickhouse): add polymarket_markets and polymarket_prices tables"
```

---

### Task 2: Watermark Extension for `condition_id`

The existing `get_watermark` supports a `market` filter (`WHERE market = {market:String}`). Polymarket needs per-market watermarking via `condition_id`. Add `condition_id` as a new optional keyword argument — `market` behavior is unchanged.

**Files:**
- Modify: `dadayu/watermark.py`
- Modify: `tests/test_watermark.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_watermark.py`:

```python
def test_get_watermark_with_condition_id_uses_param():
    import datetime
    client = _mock_client(datetime.datetime(2026, 5, 18, 12, 0, 0))
    result = get_watermark(client, "polymarket_prices", "ts", condition_id="0xabc123")
    assert result == "2026-05-19"
    call_args = client.query.call_args
    assert "{condition_id:String}" in call_args[0][0]
    assert call_args[1]["parameters"]["condition_id"] == "0xabc123"


def test_get_watermark_condition_id_returns_none_when_empty():
    client = _mock_client(None)
    result = get_watermark(client, "polymarket_prices", "ts", condition_id="0xabc123")
    assert result is None
```

- [ ] **Step 2: Run to verify they fail**

```bash
pytest tests/test_watermark.py -v -k "condition_id"
```

Expected: FAIL — `TypeError: get_watermark() got an unexpected keyword argument 'condition_id'`

- [ ] **Step 3: Update `dadayu/watermark.py`**

Replace the full file:

```python
from __future__ import annotations

import clickhouse_connect
import pandas as pd


def get_watermark(
    client: clickhouse_connect.driver.Client,
    table: str,
    date_col: str,
    market: str | None = None,
    condition_id: str | None = None,
) -> str | None:
    try:
        if market is not None:
            result = client.query(
                f"SELECT max({date_col}) FROM {table} WHERE market = {{market:String}}",
                parameters={"market": market},
            )
        elif condition_id is not None:
            result = client.query(
                f"SELECT max({date_col}) FROM {table} WHERE condition_id = {{condition_id:String}}",
                parameters={"condition_id": condition_id},
            )
        else:
            result = client.query(f"SELECT max({date_col}) FROM {table}")
        val = result.result_rows[0][0]
        if val is None:
            return None
        return (pd.Timestamp(val) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception as exc:
        print(f"  [WARN] Watermark query failed: {exc}")
        return None
```

- [ ] **Step 4: Run all watermark tests**

```bash
pytest tests/test_watermark.py -v
```

Expected: all 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add dadayu/watermark.py tests/test_watermark.py
git commit -m "feat(watermark): add condition_id parameter for per-market watermarking"
```

---

### Task 3: Polymarket Ingest Module

Create `dadayu/ingest/polymarket.py` following the same structure as `dadayu/ingest/crypto.py`. Three public functions: `discover_markets`, `fetch_price_history`, `fetch_daily_price_history`. Both fetch functions share a private `_fetch_clob_history` helper.

**Key API facts:**
- Gamma API: `GET https://gamma-api.polymarket.com/markets` — returns market list with `conditionId`, `tokens` array (YES token has `outcome: "Yes"` and `token_id`), `volume`, `liquidity`, `endDate`
- CLOB API: `GET https://clob.polymarket.com/prices-history` — takes `market_id` (the YES `token_id`, NOT the `conditionId`), returns `{"history": [{"t": unix_ts, "p": "0.65", "v": "1234.5"}, ...]}`
- Both APIs are public (no auth)
- Rate limit: 0.15s sleep between CLOB calls; 429 → exponential backoff (4^attempt seconds, max 60s), 3 retries

**Files:**
- Create: `dadayu/ingest/polymarket.py`
- Create: `tests/test_polymarket_ingest.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_polymarket_ingest.py`:

```python
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _mock_gamma_market(condition_id="0xabc123", question="Will BTC exceed $100k?"):
    return {
        "conditionId": condition_id,
        "question": question,
        "category": "Crypto",
        "volume": "75000.50",
        "liquidity": "20000.00",
        "active": True,
        "closed": False,
        "endDate": "2025-12-31T00:00:00Z",
        "outcome": None,
        "tokens": [
            {"outcome": "Yes", "token_id": "token_yes_abc"},
            {"outcome": "No", "token_id": "token_no_abc"},
        ],
    }


def _mock_clob_response(history=None):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"history": history or []}
    return mock_resp


def test_discover_markets_parses_condition_id():
    with patch("dadayu.ingest.polymarket.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [_mock_gamma_market()]
        from dadayu.ingest.polymarket import discover_markets
        df = discover_markets(min_volume_usd=50_000)
    assert len(df) == 1
    assert df.iloc[0]["condition_id"] == "0xabc123"
    assert df.iloc[0]["yes_token_id"] == "token_yes_abc"
    assert df.iloc[0]["volume_usd"] == pytest.approx(75000.50)


def test_discover_markets_auto_parses_btc_ticker():
    with patch("dadayu.ingest.polymarket.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [_mock_gamma_market(question="Will BTC exceed $100k?")]
        from dadayu.ingest.polymarket import discover_markets
        df = discover_markets(min_volume_usd=50_000)
    assert df.iloc[0]["linked_asset"] == "BTC-USD"
    assert df.iloc[0]["asset_type"] == "crypto"


def test_discover_markets_returns_none_for_unknown_asset():
    with patch("dadayu.ingest.polymarket.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [
            _mock_gamma_market(question="Will the US avoid a recession?")
        ]
        from dadayu.ingest.polymarket import discover_markets
        df = discover_markets(min_volume_usd=50_000)
    assert df.iloc[0]["linked_asset"] is None
    assert df.iloc[0]["asset_type"] is None


def test_fetch_price_history_parses_clob_response():
    history = [
        {"t": 1700000000, "p": "0.65", "v": "1234.5"},
        {"t": 1700003600, "p": "0.70", "v": "500.0"},
    ]
    with patch("dadayu.ingest.polymarket.requests.get", return_value=_mock_clob_response(history)):
        from dadayu.ingest.polymarket import fetch_price_history
        df = fetch_price_history("token_yes_abc", 1700000000, 1700010000)
    assert len(df) == 2
    assert list(df.columns) == ["ts", "probability", "volume_usd"]
    assert df.iloc[0]["probability"] == pytest.approx(0.65)
    assert df.iloc[0]["volume_usd"] == pytest.approx(1234.5)


def test_fetch_price_history_returns_empty_on_no_history():
    with patch("dadayu.ingest.polymarket.requests.get", return_value=_mock_clob_response([])):
        from dadayu.ingest.polymarket import fetch_price_history
        df = fetch_price_history("token_yes_abc", 1700000000, 1700010000)
    assert df.empty
    assert list(df.columns) == ["ts", "probability", "volume_usd"]


def test_fetch_daily_price_history_uses_fidelity_1440():
    history = [{"t": 1700000000, "p": "0.55", "v": "5000.0"}]
    with patch("dadayu.ingest.polymarket.requests.get", return_value=_mock_clob_response(history)) as mock_get:
        from dadayu.ingest.polymarket import fetch_daily_price_history
        fetch_daily_price_history("token_yes_abc", 1700000000, 1700100000)
    call_params = mock_get.call_args[1]["params"]
    assert call_params["fidelity"] == 1440
```

- [ ] **Step 2: Run to verify they fail**

```bash
pytest tests/test_polymarket_ingest.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'dadayu.ingest.polymarket'`

- [ ] **Step 3: Create `dadayu/ingest/polymarket.py`**

```python
from __future__ import annotations

import csv
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


def _parse_linked_asset(question: str) -> tuple[str | None, str | None]:
    question_upper = question.upper()
    for ticker in _load_crypto_tickers():
        if ticker in question_upper:
            return f"{ticker}-USD", "crypto"
    return None, None


def _fetch_clob_history(
    yes_token_id: str, start_ts: int, end_ts: int, fidelity: int
) -> pd.DataFrame:
    url = f"{CLOB_BASE}/prices-history"
    params = {
        "market_id": yes_token_id,
        "startTs": start_ts,
        "endTs": end_ts,
        "fidelity": fidelity,
    }
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
        df["volume_usd"] = pd.to_numeric(df.get("volume_usd", 0), errors="coerce").fillna(0.0)
        return df[["ts", "probability", "volume_usd"]]
    raise RuntimeError(f"CLOB prices-history failed for {yes_token_id} after 3 attempts")


def discover_markets(min_volume_usd: float = 50_000) -> pd.DataFrame:
    url = f"{GAMMA_BASE}/markets"
    params = {
        "active": "true",
        "closed": "false",
        "volume_num_min": min_volume_usd,
        "limit": 500,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    overrides = _load_asset_map()
    rows = []
    for m in data:
        condition_id = m.get("conditionId", "")
        yes_token_id = next(
            (t["token_id"] for t in m.get("tokens", []) if t.get("outcome") == "Yes"),
            "",
        )
        linked_asset, asset_type = _parse_linked_asset(m.get("question", ""))
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

    return pd.DataFrame(rows)


def fetch_price_history(yes_token_id: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    return _fetch_clob_history(yes_token_id, start_ts, end_ts, fidelity=60)


def fetch_daily_price_history(yes_token_id: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    return _fetch_clob_history(yes_token_id, start_ts, end_ts, fidelity=1440)
```

- [ ] **Step 4: Run all polymarket ingest tests**

```bash
pytest tests/test_polymarket_ingest.py -v
```

Expected: all 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add dadayu/ingest/polymarket.py tests/test_polymarket_ingest.py
git commit -m "feat(ingest): polymarket discover_markets and fetch_price_history"
```

---

### Task 4: Dagster Assets

Create two assets following the pattern in `dagster_pipeline/assets/crypto.py`:
- `polymarket_markets`: calls `discover_markets`, inserts into `polymarket_markets` table
- `polymarket_prices`: queries active markets from ClickHouse, fetches hourly price history per market with per-market watermarking, 90-day backfill on first run, skips closed markets

**Files:**
- Create: `dagster_pipeline/assets/polymarket.py`

- [ ] **Step 1: Create `dagster_pipeline/assets/polymarket.py`**

```python
from __future__ import annotations

import time

import pandas as pd
from dagster import asset

from dadayu.ingest.polymarket import discover_markets, fetch_price_history
from dadayu.watermark import get_watermark
from dagster_pipeline.resources import ClickhouseResource


def _ninety_days_ago_ts() -> int:
    return int((pd.Timestamp.now() - pd.Timedelta(days=90)).timestamp())


@asset(group_name="ingestion")
def polymarket_markets(clickhouse: ClickhouseResource) -> None:
    client = clickhouse.get_client()
    df = discover_markets(min_volume_usd=50_000)
    if df.empty:
        print("  No markets discovered.")
        return
    df["fetched_at"] = pd.Timestamp.now()
    cols = [
        "condition_id", "question", "category", "volume_usd", "liquidity_usd",
        "active", "closed", "resolution_date", "outcome", "yes_token_id",
        "linked_asset", "asset_type", "fetched_at",
    ]
    client.insert_df("polymarket_markets", df[cols])
    print(f"  Upserted {len(df):,} markets into polymarket_markets")


@asset(group_name="ingestion", deps=[polymarket_markets])
def polymarket_prices(clickhouse: ClickhouseResource) -> None:
    client = clickhouse.get_client()

    result = client.query(
        "SELECT condition_id, yes_token_id FROM polymarket_markets FINAL WHERE closed = false"
    )
    markets = result.result_rows
    if not markets:
        print("  No active markets found — run polymarket_markets first.")
        return

    now_ts = int(pd.Timestamp.now().timestamp())
    fallback_start = _ninety_days_ago_ts()
    total_rows = 0

    for condition_id, yes_token_id in markets:
        watermark_str = get_watermark(
            client, "polymarket_prices", "ts", condition_id=condition_id
        )
        start_ts = (
            int(pd.Timestamp(watermark_str).timestamp())
            if watermark_str is not None
            else fallback_start
        )

        if start_ts >= now_ts:
            continue

        try:
            df = fetch_price_history(yes_token_id, start_ts, now_ts)
            if not df.empty:
                df["condition_id"] = condition_id
                df["ingested_at"] = pd.Timestamp.now()
                client.insert_df(
                    "polymarket_prices",
                    df[["condition_id", "ts", "probability", "volume_usd", "ingested_at"]],
                )
                total_rows += len(df)
        except Exception as exc:
            print(f"  [WARN] Failed prices for {condition_id}: {exc}")

        time.sleep(0.15)

    print(f"  Inserted {total_rows:,} rows for {len(markets)} markets")
```

- [ ] **Step 2: Verify import**

```bash
python -c "from dagster_pipeline.assets.polymarket import polymarket_markets, polymarket_prices; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add dagster_pipeline/assets/polymarket.py
git commit -m "feat(dagster): polymarket_markets and polymarket_prices assets"
```

---

### Task 5: Dagster Job, Schedule, and Definitions

Wire polymarket assets into a job running every 4 hours (same cadence as crypto). Register job, schedule, and assets in `definitions.py`.

**Files:**
- Modify: `dagster_pipeline/schedules.py`
- Modify: `dagster_pipeline/definitions.py`

- [ ] **Step 1: Replace `dagster_pipeline/schedules.py`**

```python
from __future__ import annotations

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

from dagster_pipeline.assets.crypto import crypto_info, crypto_ohlcv
from dagster_pipeline.assets.dbt_assets import dadayu_dbt_assets
from dagster_pipeline.assets.equity import equity_ohlcv, equity_ticker_info
from dagster_pipeline.assets.polymarket import polymarket_markets, polymarket_prices

equity_job = define_asset_job(
    name="equity_job",
    selection=AssetSelection.assets(
        equity_ohlcv, equity_ticker_info, dadayu_dbt_assets
    ),
)

crypto_job = define_asset_job(
    name="crypto_job",
    selection=AssetSelection.assets(
        crypto_ohlcv, crypto_info, dadayu_dbt_assets
    ),
)

polymarket_job = define_asset_job(
    name="polymarket_job",
    selection=AssetSelection.assets(
        polymarket_markets, polymarket_prices, dadayu_dbt_assets
    ),
)

# Daily at 22:00 UTC on weekdays (after all equity markets close)
equity_schedule = ScheduleDefinition(
    job=equity_job,
    cron_schedule="0 22 * * 1-5",
)

# Every 4 hours, 24/7 (crypto never closes)
crypto_schedule = ScheduleDefinition(
    job=crypto_job,
    cron_schedule="0 */4 * * *",
)

# Every 4 hours, 24/7 (prediction markets never close)
polymarket_schedule = ScheduleDefinition(
    job=polymarket_job,
    cron_schedule="0 */4 * * *",
)
```

- [ ] **Step 2: Replace `dagster_pipeline/definitions.py`**

```python
from __future__ import annotations

from dagster import Definitions
from dagster_dbt import DbtCliResource

from dagster_pipeline.assets.crypto import crypto_info, crypto_ohlcv
from dagster_pipeline.assets.dbt_assets import DBT_PROJECT_DIR, dadayu_dbt_assets
from dagster_pipeline.assets.equity import equity_ohlcv, equity_ticker_info
from dagster_pipeline.assets.polymarket import polymarket_markets, polymarket_prices
from dagster_pipeline.resources import ClickhouseResource
from dagster_pipeline.schedules import (
    crypto_job,
    crypto_schedule,
    equity_job,
    equity_schedule,
    polymarket_job,
    polymarket_schedule,
)

defs = Definitions(
    assets=[
        equity_ohlcv,
        equity_ticker_info,
        crypto_ohlcv,
        crypto_info,
        polymarket_markets,
        polymarket_prices,
        dadayu_dbt_assets,
    ],
    resources={
        "clickhouse": ClickhouseResource(),
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
    jobs=[equity_job, crypto_job, polymarket_job],
    schedules=[equity_schedule, crypto_schedule, polymarket_schedule],
)
```

- [ ] **Step 3: Verify definitions load**

```bash
python -c "from dagster_pipeline.definitions import defs; print('jobs:', [j.name for j in defs.jobs])"
```

Expected: `jobs: ['equity_job', 'crypto_job', 'polymarket_job']`

- [ ] **Step 4: Commit**

```bash
git add dagster_pipeline/schedules.py dagster_pipeline/definitions.py
git commit -m "feat(dagster): polymarket_job and polymarket_schedule wired into definitions"
```

---

### Task 6: dbt Seed

Create the manual override CSV. Header-only on creation — rows are added as analysts discover markets with wrong auto-parse.

**Files:**
- Create: `warehouse/seeds/polymarket_asset_map.csv`

- [ ] **Step 1: Create `warehouse/seeds/polymarket_asset_map.csv`**

```csv
condition_id,linked_asset,asset_type
```

(Header-only — rows added manually when auto-parse is wrong. Example row format: `0xabc123,BTC-USD,crypto`)

- [ ] **Step 2: Commit**

```bash
git add warehouse/seeds/polymarket_asset_map.csv
git commit -m "chore: add polymarket_asset_map seed (manual override for linked_asset)"
```

---

### Task 7: dbt Staging Models

Create source declaration and two staging models following the pattern in `warehouse/models/staging/coingecko/`.

**Files:**
- Create: `warehouse/models/staging/polymarket/_sources.yml`
- Create: `warehouse/models/staging/polymarket/stg_polymarket__markets.sql`
- Create: `warehouse/models/staging/polymarket/stg_polymarket__markets.yml`
- Create: `warehouse/models/staging/polymarket/stg_polymarket__prices.sql`
- Create: `warehouse/models/staging/polymarket/stg_polymarket__prices.yml`

- [ ] **Step 1: Create `warehouse/models/staging/polymarket/_sources.yml`**

```yaml
version: 2

sources:
  - name: polymarket
    database: dadayu
    schema: dadayu
    tables:
      - name: polymarket_markets
        description: Polymarket market metadata ingested daily via Gamma API
        freshness:
          warn_after: {count: 2, period: day}
          error_after: {count: 4, period: day}
        loaded_at_field: fetched_at

      - name: polymarket_prices
        description: Polymarket hourly probability snapshots ingested via CLOB API
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 12, period: hour}
        loaded_at_field: ingested_at
```

- [ ] **Step 2: Create `warehouse/models/staging/polymarket/stg_polymarket__markets.sql`**

```sql
WITH source AS (
    SELECT * FROM {{ source('polymarket', 'polymarket_markets') }} FINAL
)

SELECT
    condition_id,
    question,
    category,
    volume_usd,
    liquidity_usd,
    active,
    closed,
    resolution_date,
    outcome,
    yes_token_id,
    linked_asset,
    asset_type,
    fetched_at
FROM source
```

- [ ] **Step 3: Create `warehouse/models/staging/polymarket/stg_polymarket__markets.yml`**

```yaml
version: 2

models:
  - name: stg_polymarket__markets
    description: Cleaned Polymarket market metadata. One row per condition_id (deduped via FINAL).
    columns:
      - name: condition_id
        description: Unique market identifier from Polymarket
        tests:
          - not_null
          - unique
      - name: question
        description: Full market question text
      - name: yes_token_id
        description: CLOB token ID for the YES outcome — used by the CLOB prices-history API. NOT the same as condition_id.
        tests:
          - not_null
      - name: linked_asset
        description: "Ticker this market correlates to (e.g. BTC-USD). Null if no clear link."
      - name: asset_type
        description: "'crypto' | 'equity' | 'macro' | null"
      - name: closed
        description: True when market has resolved. Closed markets exit the price fetch loop permanently.
```

- [ ] **Step 4: Create `warehouse/models/staging/polymarket/stg_polymarket__prices.sql`**

```sql
WITH source AS (
    SELECT * FROM {{ source('polymarket', 'polymarket_prices') }} FINAL
)

SELECT
    condition_id,
    ts,
    probability,
    volume_usd
FROM source
WHERE probability BETWEEN 0 AND 1
```

- [ ] **Step 5: Create `warehouse/models/staging/polymarket/stg_polymarket__prices.yml`**

```yaml
version: 2

models:
  - name: stg_polymarket__prices
    description: Filtered Polymarket probability snapshots. Rows outside [0, 1] excluded.
    columns:
      - name: condition_id
        description: Market identifier — joins to stg_polymarket__markets
        tests:
          - not_null
      - name: ts
        description: Timestamp of this probability observation (hourly resolution)
        tests:
          - not_null
      - name: probability
        description: Implied YES probability [0, 1]
      - name: volume_usd
        description: USD volume traded in this candle
```

- [ ] **Step 6: Compile to verify SQL**

```bash
docker compose run --rm dadayu_dbt dbt compile --select staging/polymarket
```

Expected: `Done. PASS=4 WARN=0 ERROR=0 SKIP=0 TOTAL=4`

(4 = 2 SQL models + 2 YAML schema files — dbt counts YAML as nodes in some versions. If PASS=2, that is also acceptable.)

- [ ] **Step 7: Commit**

```bash
git add warehouse/models/staging/polymarket/
git commit -m "feat(dbt): polymarket staging models and source declarations"
```

---

### Task 8: dbt Intermediate Models

Resample raw hourly prices to 1h and 1d buckets using `toStartOfHour` / `toStartOfDay` with `argMax` for close-of-period probability.

**Files:**
- Create: `warehouse/models/intermediate/int_polymarket_prices_1h.sql`
- Create: `warehouse/models/intermediate/int_polymarket_prices_1d.sql`

- [ ] **Step 1: Create `warehouse/models/intermediate/int_polymarket_prices_1h.sql`**

```sql
SELECT
    condition_id,
    toStartOfHour(ts)       AS ts,
    argMax(probability, ts) AS probability,
    sum(volume_usd)         AS volume_usd,
    false                   AS is_interpolated
FROM {{ ref('stg_polymarket__prices') }}
GROUP BY condition_id, toStartOfHour(ts)
```

- [ ] **Step 2: Create `warehouse/models/intermediate/int_polymarket_prices_1d.sql`**

```sql
SELECT
    condition_id,
    toStartOfDay(ts)        AS ts,
    argMax(probability, ts) AS probability,
    sum(volume_usd)         AS volume_usd,
    false                   AS is_interpolated
FROM {{ ref('stg_polymarket__prices') }}
GROUP BY condition_id, toStartOfDay(ts)
```

- [ ] **Step 3: Compile to verify SQL**

```bash
docker compose run --rm dadayu_dbt dbt compile --select int_polymarket_prices_1h int_polymarket_prices_1d
```

Expected: `Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2`

- [ ] **Step 4: Commit**

```bash
git add warehouse/models/intermediate/int_polymarket_prices_1h.sql \
        warehouse/models/intermediate/int_polymarket_prices_1d.sql
git commit -m "feat(dbt): polymarket hourly and daily intermediate resampling models"
```

---

### Task 9: dbt Mart and Snapshot

Create `fct_polymarket_signals` with Δp, log_odds, and `days_to_resolution` columns. Create SCD Type 2 snapshot so resolved/delisted markets remain in the historical dataset (survivorship bias mitigation).

**Key formulas:**
- `prob_change = probability - lag(probability)` — `lagInFrame(p, 1, p)` with self as default makes first-row Δp = 0
- `log_odds = ln(p / (1-p))` — clip probability to [0.01, 0.99] before computing: `log(greatest(p, 0.01) / (1.0 - least(p, 0.99)))`. In ClickHouse `log()` is natural log.
- `days_to_resolution = dateDiff('day', ts, resolution_date)` — nullable when resolution_date is null

**Files:**
- Create: `warehouse/models/marts/markets/fct_polymarket_signals.sql`
- Create: `warehouse/models/marts/markets/fct_polymarket_signals.yml`
- Create: `warehouse/snapshots/snap_dim_polymarket_market.sql`

- [ ] **Step 1: Create `warehouse/models/marts/markets/fct_polymarket_signals.sql`**

```sql
WITH hourly AS (
    SELECT * FROM {{ ref('int_polymarket_prices_1h') }}
),

markets AS (
    SELECT
        condition_id,
        question,
        linked_asset,
        asset_type,
        resolution_date
    FROM {{ ref('stg_polymarket__markets') }}
)

SELECT
    h.condition_id,
    h.ts,
    h.probability,
    h.probability - lagInFrame(h.probability, 1, h.probability) OVER w  AS prob_change,
    log(
        greatest(h.probability, 0.01) / (1.0 - least(h.probability, 0.99))
    )                                                                    AS log_odds,
    h.volume_usd,
    h.is_interpolated,
    m.question,
    m.linked_asset,
    m.asset_type,
    if(
        m.resolution_date IS NOT NULL,
        dateDiff('day', h.ts, toDateTime(m.resolution_date)),
        NULL
    )                                                                    AS days_to_resolution
FROM hourly AS h
LEFT JOIN markets AS m USING (condition_id)
WINDOW w AS (PARTITION BY h.condition_id ORDER BY h.ts)
```

- [ ] **Step 2: Create `warehouse/models/marts/markets/fct_polymarket_signals.yml`**

```yaml
version: 2

models:
  - name: fct_polymarket_signals
    description: >
      Hourly prediction market probability signals. One row per (condition_id, ts).
      Primary inputs for correlation analysis with fct_ohlcv_crypto_1h and fct_ohlcv_1h.
      Filter WHERE days_to_resolution > 2 to exclude near-expiry probability collapse.
      Filter WHERE NOT is_interpolated to exclude hours with no trades.
    columns:
      - name: condition_id
        description: Market identifier
        tests:
          - not_null
      - name: ts
        description: 1h bucket start (UTC)
        tests:
          - not_null
      - name: probability
        description: Close-of-hour implied YES probability [0, 1]
      - name: prob_change
        description: probability minus previous hour. Primary signal (Δp). Zero for first row per market.
      - name: log_odds
        description: ln(p/(1-p)) with p clipped to [0.01, 0.99]. Use for regression instead of raw probability.
      - name: volume_usd
        description: Total USD volume traded in this hour
      - name: is_interpolated
        description: True when no trade occurred in this bucket (probability carried from prior hour)
      - name: question
        description: Market question text
      - name: linked_asset
        description: "Ticker for joining with price data (e.g. BTC-USD, AAPL). Null if no clear link."
      - name: asset_type
        description: "'crypto' | 'equity' | 'macro' | null"
      - name: days_to_resolution
        description: Days until market resolves. Null when resolution_date unknown. Filter > 2 to exclude expiry noise.
```

- [ ] **Step 3: Create `warehouse/snapshots/snap_dim_polymarket_market.sql`**

```sql
{% snapshot snap_dim_polymarket_market %}

{{ config(
    target_schema='dadayu',
    unique_key='condition_id',
    strategy='check',
    check_cols=['active', 'closed', 'outcome', 'volume_usd', 'liquidity_usd', 'linked_asset', 'asset_type']
) }}

SELECT
    condition_id,
    question,
    category,
    volume_usd,
    liquidity_usd,
    active,
    closed,
    resolution_date,
    outcome,
    yes_token_id,
    linked_asset,
    asset_type,
    fetched_at
FROM {{ ref('stg_polymarket__markets') }}

{% endsnapshot %}
```

- [ ] **Step 4: Compile mart and snapshot**

```bash
docker compose run --rm dadayu_dbt dbt compile --select fct_polymarket_signals snap_dim_polymarket_market
```

Expected: `Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2`

- [ ] **Step 5: Commit**

```bash
git add warehouse/models/marts/markets/fct_polymarket_signals.sql \
        warehouse/models/marts/markets/fct_polymarket_signals.yml \
        warehouse/snapshots/snap_dim_polymarket_market.sql
git commit -m "feat(dbt): fct_polymarket_signals mart and SCD2 snapshot"
```

---

### Task 10: End-to-End Smoke Test

Rebuild the image, verify the code server loads all three jobs, run the unit test suite, and confirm Dagster UI shows `polymarket_job`.

**Files:** None — verification only.

- [ ] **Step 1: Rebuild the image**

```bash
docker compose build dadayu_dagster_code
```

Expected: exits 0, no errors.

- [ ] **Step 2: Restart code server**

```bash
docker compose restart dadayu_dagster_code
```

- [ ] **Step 3: Wait for health and check logs**

```bash
sleep 30 && docker compose logs dadayu_dagster_code --tail 30
```

Expected: no `ImportError` or `ModuleNotFoundError`. Should contain:
```
Started Dagster code server for module dagster_pipeline.definitions on 0.0.0.0:4000
```

- [ ] **Step 4: Verify ClickHouse tables**

```bash
docker exec dadayu_clickhouse clickhouse-client \
  --user dadayu --password changeme --database dadayu \
  --query "SHOW TABLES LIKE 'polymarket%'"
```

Expected:
```
polymarket_markets
polymarket_prices
```

- [ ] **Step 5: Run unit test suite**

```bash
pytest tests/ -v --ignore=tests/test_dagster_assets.py
```

Expected: all tests PASS. Suite now includes `test_watermark.py` (6 tests) and `test_polymarket_ingest.py` (6 tests).

- [ ] **Step 6: Verify Dagster UI**

Open `http://localhost:3000`. Navigate to **Jobs**. Expected: `polymarket_job` listed alongside `equity_job` and `crypto_job`. Navigate to **Assets** — `polymarket_markets` and `polymarket_prices` visible in the `ingestion` group.

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Covered in |
|---|---|
| `polymarket_markets` ClickHouse table | Task 1 |
| `polymarket_prices` ClickHouse table | Task 1 |
| `condition_id` watermark param | Task 2 |
| `discover_markets(min_volume_usd=50_000)` | Task 3 |
| `fetch_price_history(yes_token_id, start_ts, end_ts)` | Task 3 |
| `fetch_daily_price_history(yes_token_id, start_ts, end_ts)` | Task 3 |
| Auto-parse `linked_asset` from question text using crypto tickers | Task 3 |
| Manual overrides from `polymarket_asset_map.csv` | Task 3 + 6 |
| Rate limiting: 0.15s sleep, 429 exponential backoff, 3 retries | Task 3 |
| 90-day backfill fallback on first run | Task 4 |
| Per-market watermark in price asset | Task 4 |
| Skip closed markets in price fetch loop | Task 4 |
| `polymarket_markets` Dagster asset (daily discovery) | Task 4 |
| `polymarket_prices` Dagster asset (4h price fetch) | Task 4 |
| `polymarket_job` | Task 5 |
| `polymarket_schedule` (every 4 hours) | Task 5 |
| dbt source declarations | Task 7 |
| `stg_polymarket__markets` | Task 7 |
| `stg_polymarket__prices` (filter probability BETWEEN 0 AND 1) | Task 7 |
| `int_polymarket_prices_1h` | Task 8 |
| `int_polymarket_prices_1d` | Task 8 |
| `fct_polymarket_signals` with prob_change, log_odds, is_interpolated, days_to_resolution | Task 9 |
| `snap_dim_polymarket_market` SCD Type 2 | Task 9 |
| `polymarket_asset_map.csv` seed | Task 6 |

All spec requirements covered. ✅

**Type consistency:**
- `yes_token_id` is the CLOB token ID used in `fetch_price_history` — consistent across Tasks 3, 4, 7. `condition_id` is the market ID used everywhere else. Never swapped. ✅
- `get_watermark(..., condition_id=condition_id)` matches the updated signature from Task 2. ✅
- Column names in `fct_polymarket_signals` (`prob_change`, `log_odds`, `is_interpolated`, `days_to_resolution`) match spec exactly. ✅
