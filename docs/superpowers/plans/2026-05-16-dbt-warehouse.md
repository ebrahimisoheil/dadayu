# dbt Warehouse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a layered dbt warehouse on ClickHouse for equity OHLCV data (Germany, US, India) with technical indicators across 1h, 4h, and 1d intervals.

**Architecture:** Python ingestion populates raw ClickHouse tables → dbt transforms through staging/intermediate/marts layers → final views expose clean facts and indicators. Ticker metadata uses SCD2 snapshots to track changes over time.

**Tech Stack:** ClickHouse 24, dbt-clickhouse 1.8, dbt_utils, dbt_expectations, elementary, yfinance, Python 3.12, Docker Compose.

---

## File Map

**New files — Python/Docker:**
- `fetch_ticker_info.py` — yfinance metadata → ClickHouse tickers (append-only)
- `Dockerfile.dbt` — dbt-clickhouse image
- `warehouse/requirements-dbt.txt` — dbt deps

**Modified files:**
- `db/clickhouse_init.sql` — add tickers table DDL
- `api.py` — add POST /run/fetch-ticker-info
- `docker-compose.yml` — add dadayu_dbt service

**New files — dbt:**
```
warehouse/
├── dbt_project.yml
├── packages.yml
├── profiles.yml.example
├── requirements-dbt.txt
├── seeds/
│   ├── trading_calendar.csv
│   └── gics_hierarchy.csv
├── macros/
│   ├── time_bucket.sql
│   ├── ch_table_engine.sql
│   └── indicators/
│       ├── sma.sql
│       ├── ema.sql
│       ├── rsi.sql
│       ├── macd.sql
│       ├── atr.sql
│       └── bbands.sql
├── snapshots/
│   └── snap_dim_equity_symbol.sql
└── models/
    ├── staging/yahoo/
    │   ├── _sources.yml
    │   ├── _schema.yml
    │   ├── stg_yahoo__ohlcv_1h.sql
    │   ├── stg_yahoo__ohlcv_4h.sql
    │   ├── stg_yahoo__ohlcv_1d.sql
    │   └── stg_yahoo__ticker_info.sql
    ├── intermediate/
    │   ├── _schema.yml
    │   ├── int_calendar_sessions.sql
    │   ├── int_equity_ohlcv_1h.sql
    │   ├── int_equity_ohlcv_4h.sql
    │   └── int_equity_ohlcv_1d.sql
    └── marts/
        ├── reference/
        │   ├── _schema.yml
        │   ├── dim_calendar.sql
        │   └── dim_equity_symbol.sql
        ├── markets/
        │   ├── _schema.yml
        │   ├── fct_ohlcv_1h.sql
        │   ├── fct_ohlcv_4h.sql
        │   └── fct_ohlcv_1d.sql
        └── indicators/
            ├── _schema.yml
            ├── fct_indicators_1h.sql
            ├── fct_indicators_4h.sql
            └── fct_indicators_1d.sql
```

---

## Task 1: ClickHouse tickers table

**Files:**
- Modify: `db/clickhouse_init.sql`

- [ ] **Add DDL to clickhouse_init.sql** (append after existing content):

```sql
-- Equity metadata — append-only, ReplacingMergeTree deduplicates by fetched_at
CREATE TABLE IF NOT EXISTS tickers
(
    ticker      String,
    market      LowCardinality(String),
    name        String,
    sector      String,
    industry    String,
    currency    LowCardinality(String),
    country     String,
    market_cap  Nullable(Float64),
    pe_ratio    Nullable(Float64),
    fetched_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (market, ticker)
PARTITION BY market;
```

- [ ] **Create table in running ClickHouse:**

```bash
docker exec dadayu_clickhouse clickhouse-client \
  --user dadayu --password changeme --database dadayu \
  --query "
CREATE TABLE IF NOT EXISTS tickers
(
    ticker      String,
    market      LowCardinality(String),
    name        String,
    sector      String,
    industry    String,
    currency    LowCardinality(String),
    country     String,
    market_cap  Nullable(Float64),
    pe_ratio    Nullable(Float64),
    fetched_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (market, ticker)
PARTITION BY market;"
```

- [ ] **Verify:**

```bash
docker exec dadayu_clickhouse clickhouse-client \
  --user dadayu --password changeme --database dadayu \
  --query "SHOW TABLES"
```

Expected output includes `tickers`.

- [ ] **Commit:**

```bash
git add db/clickhouse_init.sql
git commit -m "feat: add tickers table DDL to ClickHouse init"
```

---

## Task 2: fetch_ticker_info.py

**Files:**
- Create: `fetch_ticker_info.py`

- [ ] **Create the script:**

```python
#!/usr/bin/env python3
"""Fetch yfinance ticker metadata and append to ClickHouse tickers table.

Always appends — never truncates. ReplacingMergeTree(fetched_at) deduplicates.
Run after any universe change to keep metadata fresh.

Usage:
  python fetch_ticker_info.py                  # all markets
  python fetch_ticker_info.py --market us
"""

from __future__ import annotations

import argparse
import os
import time

import clickhouse_connect
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

MARKETS = ["germany", "us", "india"]


def get_ch_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        database=os.environ.get("CLICKHOUSE_DB", "dadayu"),
        username=os.environ.get("CLICKHOUSE_USER", "dadayu"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


YFINANCE_FIELDS = {
    "longName":      "name",
    "sector":        "sector",
    "industry":      "industry",
    "currency":      "currency",
    "country":       "country",
    "marketCap":     "market_cap",
    "trailingPE":    "pe_ratio",
}


def fetch_market_tickers(market: str) -> list[str]:
    client = get_ch_client()
    result = client.query(
        "SELECT DISTINCT ticker FROM prices_daily WHERE market = {market:String}",
        parameters={"market": market},
    )
    return [row[0] for row in result.result_rows]


def fetch_metadata(tickers: list[str], market: str) -> pd.DataFrame:
    records = []
    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i}/{len(tickers)}] {ticker}", end="\r")
        row = {"ticker": ticker, "market": market}
        try:
            info = yf.Ticker(ticker).info
            for yf_key, col in YFINANCE_FIELDS.items():
                row[col] = info.get(yf_key)
        except Exception as exc:
            print(f"\n  [WARN] {ticker}: {exc}")
            for col in YFINANCE_FIELDS.values():
                row.setdefault(col, None)
        records.append(row)
        time.sleep(0.1)  # rate limit
    print()
    return pd.DataFrame(records)


def insert_metadata(df: pd.DataFrame) -> None:
    client = get_ch_client()
    df = df.copy()
    df["fetched_at"] = pd.Timestamp.now()

    # Ensure correct dtypes
    for col in ["name", "sector", "industry", "currency", "country"]:
        df[col] = df[col].fillna("").astype(str)
    for col in ["market_cap", "pe_ratio"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    cols = ["ticker", "market", "name", "sector", "industry",
            "currency", "country", "market_cap", "pe_ratio", "fetched_at"]
    client.insert_df("tickers", df[cols])
    print(f"  Inserted {len(df)} rows into tickers [{df['market'].iloc[0]}]")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch ticker metadata → ClickHouse tickers")
    parser.add_argument("--market", choices=MARKETS + ["all"], default="all")
    args = parser.parse_args()

    markets = MARKETS if args.market == "all" else [args.market]
    for market in markets:
        print(f"\n=== {market.upper()} — fetching metadata ===")
        tickers = fetch_market_tickers(market)
        if not tickers:
            print(f"  [ERROR] No tickers found for {market} in prices_daily. Run fetch_hourly_prices.py first.")
            continue
        print(f"  Found {len(tickers)} tickers")
        df = fetch_metadata(tickers, market)
        insert_metadata(df)

    print("\nDone.")


if __name__ == "__main__":
    main()
```

- [ ] **Commit:**

```bash
git add fetch_ticker_info.py
git commit -m "feat: add fetch_ticker_info.py — yfinance metadata → ClickHouse tickers"
```

---

## Task 3: API endpoint for ticker info

**Files:**
- Modify: `api.py`

- [ ] **Add endpoint to api.py** (after the existing `/run/fetch-prices` endpoint):

```python
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
```

- [ ] **Commit:**

```bash
git add api.py
git commit -m "feat: add POST /run/fetch-ticker-info API endpoint"
```

---

## Task 4: Run ticker ingestion and verify

- [ ] **Rebuild API container:**

```bash
cd "/Users/soheilebrahimi/Documents/DADAYU AI"
docker compose up -d --build dadayu_api
```

- [ ] **Trigger fetch for all markets (runs ~10–20 min for 1000+ tickers):**

```bash
curl -s -X POST "http://localhost:8000/run/fetch-ticker-info?market=all" \
  --max-time 3600
```

Expected: `{"returncode": 0, "stdout": "...Inserted N rows into tickers [germany]..."}` for each market.

- [ ] **Verify tickers table:**

```bash
docker exec dadayu_clickhouse clickhouse-client \
  --user dadayu --password changeme --database dadayu \
  --query "
SELECT market, count() AS rows, countDistinct(ticker) AS tickers,
       countIf(sector != '') AS with_sector
FROM tickers FINAL
GROUP BY market ORDER BY market"
```

Expected: 3 rows, each with 80–500 tickers depending on market.

- [ ] **Commit:**

```bash
git commit --allow-empty -m "chore: ticker metadata ingested into ClickHouse tickers table"
```

---

## Task 5: dbt project scaffold

**Files:**
- Create: `Dockerfile.dbt`
- Create: `warehouse/requirements-dbt.txt`
- Create: `warehouse/dbt_project.yml`
- Create: `warehouse/packages.yml`
- Create: `warehouse/profiles.yml.example`
- Modify: `docker-compose.yml`

- [ ] **Create Dockerfile.dbt:**

```dockerfile
FROM python:3.12-slim
WORKDIR /usr/app/dbt
COPY warehouse/requirements-dbt.txt .
RUN pip install --no-cache-dir -r requirements-dbt.txt
VOLUME ["/usr/app/dbt", "/root/.dbt"]
ENTRYPOINT ["dbt"]
CMD ["--help"]
```

- [ ] **Create warehouse/requirements-dbt.txt:**

```
dbt-clickhouse==1.8.0
dbt-utils==1.3.0
```

- [ ] **Create warehouse/dbt_project.yml:**

```yaml
name: dadayu_warehouse
version: '1.0.0'
config-version: 2

profile: dadayu

model-paths: ["models"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
docs-paths: ["docs"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  dadayu_warehouse:
    staging:
      +materialized: view
    intermediate:
      +materialized: view
    marts:
      reference:
        +materialized: table
        +engine: ReplacingMergeTree()
      markets:
        +materialized: view
      indicators:
        +materialized: view

seeds:
  dadayu_warehouse:
    trading_calendar:
      +column_types:
        is_trading_day: UInt8
    gics_hierarchy:
      +column_types:
        sector_id: String
        industry_group_id: String
        industry_id: String
        sub_industry_id: String

snapshots:
  dadayu_warehouse:
    +target_schema: dadayu
```

- [ ] **Create warehouse/packages.yml:**

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<1.0.0"]
  - package: elementary-data/elementary
    version: [">=0.15.0", "<1.0.0"]
```

- [ ] **Create warehouse/profiles.yml.example:**

```yaml
dadayu:
  target: dev
  outputs:
    dev:
      type: clickhouse
      schema: dadayu
      host: dadayu_clickhouse   # use 'localhost' when running dbt outside Docker
      port: 8123
      user: dadayu
      password: changeme        # replace with actual password from .env
      secure: false
      connect_timeout: 10
      send_receive_timeout: 300
```

- [ ] **Copy to ~/.dbt/profiles.yml (actual credentials, not committed):**

```bash
mkdir -p ~/.dbt
cp "/Users/soheilebrahimi/Documents/DADAYU AI/warehouse/profiles.yml.example" ~/.dbt/profiles.yml
# Edit host to 'localhost' if running dbt locally, or 'dadayu_clickhouse' if inside Docker
```

- [ ] **Add dadayu_dbt service to docker-compose.yml** (append to services block):

```yaml
  dadayu_dbt:
    build:
      context: .
      dockerfile: Dockerfile.dbt
    container_name: dadayu_dbt
    volumes:
      - ./warehouse:/usr/app/dbt
      - ~/.dbt:/root/.dbt:ro
    working_dir: /usr/app/dbt
    depends_on:
      - dadayu_clickhouse
    profiles:
      - tools
```

- [ ] **Create warehouse directory structure:**

```bash
mkdir -p "/Users/soheilebrahimi/Documents/DADAYU AI/warehouse/"{seeds,macros/indicators,snapshots,models/staging/yahoo,models/intermediate,models/marts/reference,models/marts/markets,models/marts/indicators}
```

- [ ] **Install dbt packages:**

```bash
cd "/Users/soheilebrahimi/Documents/DADAYU AI"
docker compose --profile tools run --rm dadayu_dbt dbt deps
```

Expected: `[OK found compatible packages]` and `dbt_packages/` created inside `warehouse/`.

- [ ] **Verify dbt can connect:**

```bash
docker compose --profile tools run --rm dadayu_dbt dbt debug
```

Expected: `All checks passed!`

- [ ] **Commit:**

```bash
git add Dockerfile.dbt warehouse/ docker-compose.yml
git commit -m "feat: scaffold dbt project with ClickHouse adapter and Docker service"
```

---

## Task 6: Seeds

**Files:**
- Create: `warehouse/seeds/trading_calendar.csv`
- Create: `warehouse/seeds/gics_hierarchy.csv`

- [ ] **Create warehouse/seeds/trading_calendar.csv:**

```csv
date,market,is_trading_day,session_open_utc,session_close_utc
2026-04-01,germany,1,07:00:00,15:30:00
2026-04-01,us,1,14:30:00,21:00:00
2026-04-01,india,1,03:45:00,10:00:00
2026-04-02,germany,1,07:00:00,15:30:00
2026-04-02,us,1,14:30:00,21:00:00
2026-04-02,india,1,03:45:00,10:00:00
2026-04-03,germany,0,,,
2026-04-03,us,0,,,
2026-04-03,india,0,,,
2026-04-04,germany,0,,,
2026-04-04,us,0,,,
2026-04-04,india,0,,,
2026-04-05,germany,0,,,
2026-04-05,us,0,,,
2026-04-05,india,0,,,
2026-04-06,germany,0,,,
2026-04-06,us,1,14:30:00,21:00:00
2026-04-06,india,0,,,
2026-04-07,germany,1,07:00:00,15:30:00
2026-04-07,us,1,14:30:00,21:00:00
2026-04-07,india,1,03:45:00,10:00:00
2026-04-08,germany,1,07:00:00,15:30:00
2026-04-08,us,1,14:30:00,21:00:00
2026-04-08,india,1,03:45:00,10:00:00
2026-04-09,germany,1,07:00:00,15:30:00
2026-04-09,us,1,14:30:00,21:00:00
2026-04-09,india,1,03:45:00,10:00:00
2026-04-10,germany,1,07:00:00,15:30:00
2026-04-10,us,1,14:30:00,21:00:00
2026-04-10,india,1,03:45:00,10:00:00
2026-04-11,germany,0,,,
2026-04-11,us,0,,,
2026-04-11,india,0,,,
2026-04-12,germany,0,,,
2026-04-12,us,0,,,
2026-04-12,india,0,,,
2026-04-13,germany,1,07:00:00,15:30:00
2026-04-13,us,1,14:30:00,21:00:00
2026-04-13,india,1,03:45:00,10:00:00
2026-04-14,germany,1,07:00:00,15:30:00
2026-04-14,us,1,14:30:00,21:00:00
2026-04-14,india,0,,,
2026-04-15,germany,1,07:00:00,15:30:00
2026-04-15,us,1,14:30:00,21:00:00
2026-04-15,india,1,03:45:00,10:00:00
2026-04-16,germany,1,07:00:00,15:30:00
2026-04-16,us,1,14:30:00,21:00:00
2026-04-16,india,1,03:45:00,10:00:00
2026-04-17,germany,1,07:00:00,15:30:00
2026-04-17,us,1,14:30:00,21:00:00
2026-04-17,india,1,03:45:00,10:00:00
2026-04-18,germany,0,,,
2026-04-18,us,0,,,
2026-04-18,india,0,,,
2026-04-19,germany,0,,,
2026-04-19,us,0,,,
2026-04-19,india,0,,,
2026-04-20,germany,1,07:00:00,15:30:00
2026-04-20,us,1,14:30:00,21:00:00
2026-04-20,india,1,03:45:00,10:00:00
2026-04-21,germany,1,07:00:00,15:30:00
2026-04-21,us,1,14:30:00,21:00:00
2026-04-21,india,1,03:45:00,10:00:00
2026-04-22,germany,1,07:00:00,15:30:00
2026-04-22,us,1,14:30:00,21:00:00
2026-04-22,india,1,03:45:00,10:00:00
2026-04-23,germany,1,07:00:00,15:30:00
2026-04-23,us,1,14:30:00,21:00:00
2026-04-23,india,1,03:45:00,10:00:00
2026-04-24,germany,1,07:00:00,15:30:00
2026-04-24,us,1,14:30:00,21:00:00
2026-04-24,india,1,03:45:00,10:00:00
2026-04-25,germany,0,,,
2026-04-25,us,0,,,
2026-04-25,india,0,,,
2026-04-26,germany,0,,,
2026-04-26,us,0,,,
2026-04-26,india,0,,,
2026-04-27,germany,1,07:00:00,15:30:00
2026-04-27,us,1,14:30:00,21:00:00
2026-04-27,india,1,03:45:00,10:00:00
2026-04-28,germany,1,07:00:00,15:30:00
2026-04-28,us,1,14:30:00,21:00:00
2026-04-28,india,1,03:45:00,10:00:00
2026-04-29,germany,1,07:00:00,15:30:00
2026-04-29,us,1,14:30:00,21:00:00
2026-04-29,india,1,03:45:00,10:00:00
2026-04-30,germany,1,07:00:00,15:30:00
2026-04-30,us,1,14:30:00,21:00:00
2026-04-30,india,1,03:45:00,10:00:00
```

- [ ] **Create warehouse/seeds/gics_hierarchy.csv** (GICS 2023 standard, 11 sectors):

```csv
sector_id,sector_name,industry_group_id,industry_group_name,industry_id,industry_name,sub_industry_id,sub_industry_name
10,Energy,1010,Energy,101010,Energy Equipment & Services,10101010,Oil & Gas Drilling
10,Energy,1010,Energy,101010,Energy Equipment & Services,10101020,Oil Gas & Consumable Fuels
10,Energy,1010,Energy,101020,Oil Gas & Consumable Fuels,10102010,Integrated Oil & Gas
15,Materials,1510,Materials,151010,Chemicals,15101010,Commodity Chemicals
15,Materials,1510,Materials,151010,Chemicals,15101020,Diversified Chemicals
15,Materials,1510,Materials,151020,Construction Materials,15102010,Construction Materials
20,Industrials,2010,Capital Goods,201010,Aerospace & Defense,20101010,Aerospace & Defense
20,Industrials,2010,Capital Goods,201020,Building Products,20102010,Building Products
20,Industrials,2010,Capital Goods,201030,Construction & Engineering,20103010,Construction & Engineering
20,Industrials,2020,Commercial & Professional Services,202010,Commercial Services & Supplies,20201010,Commercial Printing
20,Industrials,2030,Transportation,203010,Air Freight & Logistics,20301010,Air Freight & Logistics
25,Consumer Discretionary,2510,Automobiles & Components,251010,Auto Components,25101010,Auto Parts & Equipment
25,Consumer Discretionary,2510,Automobiles & Components,251020,Automobiles,25102010,Automobile Manufacturers
25,Consumer Discretionary,2520,Consumer Durables & Apparel,252010,Household Durables,25201010,Consumer Electronics
25,Consumer Discretionary,2530,Consumer Services,253010,Hotels Restaurants & Leisure,25301010,Casinos & Gaming
25,Consumer Discretionary,2550,Retailing,255010,Distributors,25501010,Distributors
25,Consumer Discretionary,2550,Retailing,255040,Specialty Retail,25504010,Apparel Retail
30,Consumer Staples,3010,Food & Staples Retailing,301010,Food & Staples Retailing,30101010,Drug Retail
30,Consumer Staples,3020,Food Beverage & Tobacco,302010,Beverages,30201010,Brewers
30,Consumer Staples,3020,Food Beverage & Tobacco,302020,Food Products,30202010,Agricultural Products
35,Health Care,3510,Health Care Equipment & Services,351010,Health Care Equipment & Supplies,35101010,Health Care Equipment
35,Health Care,3510,Health Care Equipment & Services,351020,Health Care Providers & Services,35102010,Health Care Facilities
35,Health Care,3520,Pharmaceuticals Biotechnology & Life Sciences,352010,Biotechnology,35201010,Biotechnology
35,Health Care,3520,Pharmaceuticals Biotechnology & Life Sciences,352020,Pharmaceuticals,35202010,Pharmaceuticals
40,Financials,4010,Banks,401010,Banks,40101010,Diversified Banks
40,Financials,4010,Banks,401010,Banks,40101015,Regional Banks
40,Financials,4020,Diversified Financials,402010,Diversified Financial Services,40201020,Multi-Sector Holdings
40,Financials,4030,Insurance,403010,Insurance,40301010,Insurance Brokers
45,Information Technology,4510,Software & Services,451020,IT Services,45102010,IT Consulting & Other Services
45,Information Technology,4510,Software & Services,451030,Software,45103010,Application Software
45,Information Technology,4520,Technology Hardware & Equipment,452010,Communications Equipment,45201020,Communications Equipment
45,Information Technology,4530,Semiconductors & Semiconductor Equipment,453010,Semiconductors,45301010,Semiconductor Equipment
50,Communication Services,5010,Telecommunication Services,501010,Diversified Telecommunication Services,50101010,Alternative Carriers
50,Communication Services,5010,Telecommunication Services,501020,Wireless Telecommunication Services,50102010,Wireless Telecommunication Services
50,Communication Services,5020,Media & Entertainment,502010,Media,50201010,Advertising
55,Utilities,5510,Utilities,551010,Electric Utilities,55101010,Electric Utilities
55,Utilities,5510,Utilities,551020,Gas Utilities,55102010,Gas Utilities
55,Utilities,5510,Utilities,551030,Multi-Utilities,55103010,Multi-Utilities
60,Real Estate,6010,Real Estate,601010,Equity Real Estate Investment Trusts,60101010,Diversified REITs
60,Real Estate,6010,Real Estate,601020,Real Estate Management & Development,60102010,Diversified Real Estate Activities
```

- [ ] **Run dbt seed:**

```bash
docker compose --profile tools run --rm dadayu_dbt dbt seed
```

Expected output: `Completed successfully` with 2 seeds loaded.

- [ ] **Verify seeds in ClickHouse:**

```bash
docker exec dadayu_clickhouse clickhouse-client \
  --user dadayu --password changeme --database dadayu \
  --query "SELECT count() FROM trading_calendar; SELECT count() FROM gics_hierarchy"
```

Expected: `90` and `40` (or similar).

- [ ] **Commit:**

```bash
git add warehouse/seeds/
git commit -m "feat: add trading_calendar and gics_hierarchy seeds"
```

---

## Task 7: Core macros

**Files:**
- Create: `warehouse/macros/time_bucket.sql`
- Create: `warehouse/macros/ch_table_engine.sql`

- [ ] **Create warehouse/macros/time_bucket.sql:**

```sql
{% macro time_bucket(column, interval_str) %}
    toStartOfInterval({{ column }}, INTERVAL {{ interval_str }})
{% endmacro %}
```

- [ ] **Create warehouse/macros/ch_table_engine.sql:**

```sql
{#
  Returns ClickHouse table DDL clauses for materialized models.
  Usage in model config:
    {{ config(
        materialized='table',
        engine=ch_table_engine_str('ReplacingMergeTree', 'ingested_at'),
        order_by='(market, ticker, ts)',
        partition_by='toYYYYMM(ts)'
    ) }}
  Note: dbt-clickhouse accepts engine/order_by/partition_by directly in config().
  This macro documents the standard pattern for reference.
#}
{% macro ch_table_engine_str(engine_name='ReplacingMergeTree', version_col='ingested_at') %}
    {{ engine_name }}({{ version_col }})
{% endmacro %}
```

- [ ] **Verify macros compile (no models yet, just check syntax):**

```bash
docker compose --profile tools run --rm dadayu_dbt dbt compile --select tag:nonexistent 2>&1 | grep -i error
```

Expected: no errors (empty output or "Nothing to do").

- [ ] **Commit:**

```bash
git add warehouse/macros/time_bucket.sql warehouse/macros/ch_table_engine.sql
git commit -m "feat: add time_bucket and ch_table_engine macros"
```

---

## Task 8: Indicator macros

**Files:**
- Create: `warehouse/macros/indicators/sma.sql`
- Create: `warehouse/macros/indicators/ema.sql`
- Create: `warehouse/macros/indicators/rsi.sql`
- Create: `warehouse/macros/indicators/macd.sql`
- Create: `warehouse/macros/indicators/atr.sql`
- Create: `warehouse/macros/indicators/bbands.sql`

- [ ] **Create warehouse/macros/indicators/sma.sql:**

```sql
{#
  Simple Moving Average over n periods.
  partition_cols: comma-separated columns defining the series (e.g. 'ticker, market')
  ts: ordering column
#}
{% macro sma(col, n, ts='ts', partition_cols='ticker, market') %}
    avg({{ col }}) OVER (
        PARTITION BY {{ partition_cols }}
        ORDER BY {{ ts }}
        ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}
```

- [ ] **Create warehouse/macros/indicators/ema.sql:**

```sql
{#
  Exponential Moving Average via ClickHouse arrayFold (requires CH 22.6+).
  Returns a scalar expression referencing precomputed ema_arr column.
  Usage: include the ema_base CTE in your model, then call {{ ema_val('ema20_arr', 'idx') }}.

  The full pattern for a model using EMA:
    WITH base AS (SELECT ticker, market, ts, close FROM ...)
    , ema_grouped AS (
        SELECT ticker, market,
               groupArray(close) AS close_arr,
               groupArray(ts) AS ts_arr
        FROM base GROUP BY ticker, market
    )
    , ema_calc AS (
        SELECT ticker, market, ts_arr,
               {{ ema_compute('close_arr', 20) }} AS ema20_arr,
               {{ ema_compute('close_arr', 12) }} AS ema12_arr,
               {{ ema_compute('close_arr', 26) }} AS ema26_arr
        FROM ema_grouped
    )
    , ema_flat AS (
        SELECT ticker, market,
               ts_arr[idx] AS ts,
               ema20_arr[idx] AS ema_20,
               ema12_arr[idx] AS ema_12,
               ema26_arr[idx] AS ema_26
        FROM ema_calc
        ARRAY JOIN arrayEnumerate(ts_arr) AS idx
    )
#}
{% macro ema_compute(close_arr_col, n) %}
    arrayFold(
        (acc, x) -> arrayPushBack(
            acc,
            arrayElement(acc, -1) * (1.0 - {{ 2.0 / (n + 1) }}) + x * {{ 2.0 / (n + 1) }}
        ),
        {{ close_arr_col }},
        [toFloat64(arrayElement({{ close_arr_col }}, 1))]
    )
{% endmacro %}
```

- [ ] **Create warehouse/macros/indicators/rsi.sql:**

```sql
{#
  Relative Strength Index (simplified: SMA of gains / SMA of losses).
  Requires a 'close_diff' column (close - lag(close)) in the input.
  n: lookback period (standard: 14)
#}
{% macro rsi(n=14, ts='ts', partition_cols='ticker, market') %}
    100.0 - 100.0 / (
        1.0 + (
            avg(if(close_diff > 0, close_diff, 0.0)) OVER (
                PARTITION BY {{ partition_cols }}
                ORDER BY {{ ts }}
                ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
            )
            /
            nullIf(
                avg(if(close_diff < 0, abs(close_diff), 0.0)) OVER (
                    PARTITION BY {{ partition_cols }}
                    ORDER BY {{ ts }}
                    ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
                ),
                0.0
            )
        )
    )
{% endmacro %}
```

- [ ] **Create warehouse/macros/indicators/macd.sql:**

```sql
{#
  MACD uses precomputed EMA arrays from the ema_flat CTE.
  After joining ema_flat, MACD = ema_fast - ema_slow.
  Signal line = EMA(macd_line, signal_period) — approximated here as SMA for MVP.
  Histogram = macd_line - signal_line.
  Standard params: fast=12, slow=26, signal=9.
  Call after ema_flat CTE is available in the model.
#}
{% macro macd_line(ema_fast_col='ema_12', ema_slow_col='ema_26') %}
    ({{ ema_fast_col }} - {{ ema_slow_col }})
{% endmacro %}

{% macro macd_signal(macd_col='macd_line', n=9, ts='ts', partition_cols='ticker, market') %}
    avg({{ macd_col }}) OVER (
        PARTITION BY {{ partition_cols }}
        ORDER BY {{ ts }}
        ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

{% macro macd_hist(macd_col='macd_line', signal_col='macd_signal') %}
    ({{ macd_col }} - {{ signal_col }})
{% endmacro %}
```

- [ ] **Create warehouse/macros/indicators/atr.sql:**

```sql
{#
  Average True Range over n periods.
  Requires high, low, close, and prev_close columns.
  true_range = max(high-low, |high-prev_close|, |low-prev_close|)
  n: lookback period (standard: 14)
#}
{% macro atr(n=14, ts='ts', partition_cols='ticker, market') %}
    avg(
        greatest(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
    ) OVER (
        PARTITION BY {{ partition_cols }}
        ORDER BY {{ ts }}
        ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}
```

- [ ] **Create warehouse/macros/indicators/bbands.sql:**

```sql
{#
  Bollinger Bands.
  Returns three expressions: upper, middle (SMA), lower.
  n: SMA period (standard: 20)
  k: standard deviation multiplier (standard: 2.0)
#}
{% macro bb_middle(col, n=20, ts='ts', partition_cols='ticker, market') %}
    avg({{ col }}) OVER (
        PARTITION BY {{ partition_cols }}
        ORDER BY {{ ts }}
        ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

{% macro bb_std(col, n=20, ts='ts', partition_cols='ticker, market') %}
    stddevPop({{ col }}) OVER (
        PARTITION BY {{ partition_cols }}
        ORDER BY {{ ts }}
        ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

{% macro bb_upper(col, n=20, k=2.0, ts='ts', partition_cols='ticker, market') %}
    ({{ bb_middle(col, n, ts, partition_cols) }} + {{ k }} * {{ bb_std(col, n, ts, partition_cols) }})
{% endmacro %}

{% macro bb_lower(col, n=20, k=2.0, ts='ts', partition_cols='ticker, market') %}
    ({{ bb_middle(col, n, ts, partition_cols) }} - {{ k }} * {{ bb_std(col, n, ts, partition_cols) }})
{% endmacro %}
```

- [ ] **Verify macros compile:**

```bash
docker compose --profile tools run --rm dadayu_dbt dbt compile 2>&1 | tail -5
```

Expected: `Done.` with no errors.

- [ ] **Commit:**

```bash
git add warehouse/macros/indicators/
git commit -m "feat: add SMA, EMA, RSI, MACD, ATR, BBands indicator macros"
```

---

## Task 9: Staging layer

**Files:**
- Create: `warehouse/models/staging/yahoo/_sources.yml`
- Create: `warehouse/models/staging/yahoo/_schema.yml`
- Create: `warehouse/models/staging/yahoo/stg_yahoo__ohlcv_1h.sql`
- Create: `warehouse/models/staging/yahoo/stg_yahoo__ohlcv_4h.sql`
- Create: `warehouse/models/staging/yahoo/stg_yahoo__ohlcv_1d.sql`
- Create: `warehouse/models/staging/yahoo/stg_yahoo__ticker_info.sql`

- [ ] **Create _sources.yml:**

```yaml
version: 2

sources:
  - name: yahoo
    database: dadayu
    schema: dadayu
    tables:
      - name: prices_hourly
        identifier: prices_hourly
        loaded_at_field: ingested_at
        freshness:
          warn_after: {count: 7, period: day}
      - name: prices_4h
        identifier: prices_4h
        loaded_at_field: ingested_at
        freshness:
          warn_after: {count: 7, period: day}
      - name: prices_daily
        identifier: prices_daily
        loaded_at_field: ingested_at
        freshness:
          warn_after: {count: 2, period: day}
          error_after: {count: 4, period: day}
      - name: tickers
        identifier: tickers
        loaded_at_field: fetched_at
        freshness:
          warn_after: {count: 30, period: day}
```

- [ ] **Create _schema.yml:**

```yaml
version: 2

models:
  - name: stg_yahoo__ohlcv_1d
    description: Daily OHLCV — renamed and cast from prices_daily source
    columns:
      - name: ticker
        tests: [not_null]
      - name: market
        tests:
          - not_null
          - accepted_values:
              values: [germany, us, india]
      - name: ts
        tests: [not_null]
      - name: close
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              strictly: true

  - name: stg_yahoo__ticker_info
    description: Latest ticker metadata from tickers FINAL
    columns:
      - name: ticker
        tests: [not_null]
      - name: market
        tests: [not_null]
```

- [ ] **Create stg_yahoo__ohlcv_1h.sql:**

```sql
SELECT
    ticker,
    market,
    datetime                AS ts,
    open,
    high,
    low,
    close,
    toUInt64(volume)        AS volume
FROM {{ source('yahoo', 'prices_hourly') }}
```

- [ ] **Create stg_yahoo__ohlcv_4h.sql:**

```sql
SELECT
    ticker,
    market,
    datetime                AS ts,
    open,
    high,
    low,
    close,
    toUInt64(volume)        AS volume
FROM {{ source('yahoo', 'prices_4h') }}
```

- [ ] **Create stg_yahoo__ohlcv_1d.sql:**

```sql
SELECT
    ticker,
    market,
    toDateTime(date)        AS ts,
    open,
    high,
    low,
    close,
    toUInt64(volume)        AS volume
FROM {{ source('yahoo', 'prices_daily') }}
```

- [ ] **Create stg_yahoo__ticker_info.sql:**

```sql
SELECT
    ticker,
    market,
    name,
    sector,
    industry,
    currency,
    country,
    market_cap,
    pe_ratio,
    fetched_at
FROM {{ source('yahoo', 'tickers') }} FINAL
```

- [ ] **Run and test staging:**

```bash
docker compose --profile tools run --rm dadayu_dbt dbt run --select staging
docker compose --profile tools run --rm dadayu_dbt dbt test --select staging
```

Expected: all models created as views, all tests pass.

- [ ] **Commit:**

```bash
git add warehouse/models/staging/
git commit -m "feat: add Yahoo Finance staging models (1h, 4h, 1d OHLCV + ticker info)"
```

---

## Task 10: Intermediate layer

**Files:**
- Create: `warehouse/models/intermediate/_schema.yml`
- Create: `warehouse/models/intermediate/int_calendar_sessions.sql`
- Create: `warehouse/models/intermediate/int_equity_ohlcv_1h.sql`
- Create: `warehouse/models/intermediate/int_equity_ohlcv_4h.sql`
- Create: `warehouse/models/intermediate/int_equity_ohlcv_1d.sql`

- [ ] **Create _schema.yml:**

```yaml
version: 2

models:
  - name: int_calendar_sessions
    description: One row per (date, market) with session metadata
    columns:
      - name: date
        tests: [not_null]
      - name: market
        tests: [not_null]
      - name: session_id
        tests: [not_null, unique]

  - name: int_equity_ohlcv_1d
    description: Daily OHLCV joined with calendar sessions
    columns:
      - name: ticker
        tests: [not_null]
      - name: ts
        tests: [not_null]
```

- [ ] **Create int_calendar_sessions.sql:**

```sql
SELECT
    date,
    market,
    is_trading_day,
    session_open_utc,
    session_close_utc,
    concat(
        upper(market), '_',
        toString(toYear(date)),
        leftPad(toString(toMonth(date)), 2, '0'),
        leftPad(toString(toDayOfMonth(date)), 2, '0')
    )                           AS session_id
FROM {{ ref('trading_calendar') }}
```

- [ ] **Create int_equity_ohlcv_1h.sql:**

```sql
SELECT
    o.ticker,
    o.market,
    o.ts,
    o.open,
    o.high,
    o.low,
    o.close,
    o.volume,
    c.session_id,
    c.is_trading_day,
    c.session_open_utc,
    c.session_close_utc
FROM {{ ref('stg_yahoo__ohlcv_1h') }} AS o
LEFT JOIN {{ ref('int_calendar_sessions') }} AS c
    ON toDate(o.ts) = c.date
    AND o.market    = c.market
```

- [ ] **Create int_equity_ohlcv_4h.sql:**

```sql
SELECT
    o.ticker,
    o.market,
    o.ts,
    o.open,
    o.high,
    o.low,
    o.close,
    o.volume,
    c.session_id,
    c.is_trading_day,
    c.session_open_utc,
    c.session_close_utc
FROM {{ ref('stg_yahoo__ohlcv_4h') }} AS o
LEFT JOIN {{ ref('int_calendar_sessions') }} AS c
    ON toDate(o.ts) = c.date
    AND o.market    = c.market
```

- [ ] **Create int_equity_ohlcv_1d.sql:**

```sql
SELECT
    o.ticker,
    o.market,
    o.ts,
    o.open,
    o.high,
    o.low,
    o.close,
    o.volume,
    c.session_id,
    c.is_trading_day,
    c.session_open_utc,
    c.session_close_utc
FROM {{ ref('stg_yahoo__ohlcv_1d') }} AS o
LEFT JOIN {{ ref('int_calendar_sessions') }} AS c
    ON toDate(o.ts) = c.date
    AND o.market    = c.market
```

- [ ] **Run and test intermediate:**

```bash
docker compose --profile tools run --rm dadayu_dbt dbt run --select intermediate
docker compose --profile tools run --rm dadayu_dbt dbt test --select intermediate
```

Expected: all models created as views, all tests pass.

- [ ] **Commit:**

```bash
git add warehouse/models/intermediate/
git commit -m "feat: add intermediate equity OHLCV models with calendar session join"
```

---

## Task 11: Snapshot

**Files:**
- Create: `warehouse/snapshots/snap_dim_equity_symbol.sql`

- [ ] **Create snap_dim_equity_symbol.sql:**

```sql
{% snapshot snap_dim_equity_symbol %}

{{ config(
    target_schema='dadayu',
    unique_key=dbt_utils.generate_surrogate_key(['ticker', 'market']),
    strategy='check',
    check_cols=['name', 'sector', 'industry', 'market_cap']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ticker', 'market']) }} AS equity_id,
    ticker,
    market,
    name,
    sector,
    industry,
    currency,
    country,
    market_cap,
    pe_ratio,
    fetched_at
FROM {{ ref('stg_yahoo__ticker_info') }}

{% endsnapshot %}
```

- [ ] **Run snapshot:**

```bash
docker compose --profile tools run --rm dadayu_dbt dbt snapshot
```

Expected: `Completed successfully` — creates `snap_dim_equity_symbol` table in ClickHouse.

- [ ] **Verify:**

```bash
docker exec dadayu_clickhouse clickhouse-client \
  --user dadayu --password changeme --database dadayu \
  --query "SELECT count(), countIf(dbt_valid_to = '9999-12-31 00:00:00') AS current_rows
           FROM snap_dim_equity_symbol"
```

Expected: all rows are current (dbt_valid_to = max sentinel).

- [ ] **Commit:**

```bash
git add warehouse/snapshots/
git commit -m "feat: add SCD2 snapshot for equity symbol dimension"
```

---

## Task 12: Reference marts

**Files:**
- Create: `warehouse/models/marts/reference/_schema.yml`
- Create: `warehouse/models/marts/reference/dim_calendar.sql`
- Create: `warehouse/models/marts/reference/dim_equity_symbol.sql`

- [ ] **Create _schema.yml:**

```yaml
version: 2

models:
  - name: dim_calendar
    description: Calendar dimension with session metadata per (date, market)
    columns:
      - name: date
        tests: [not_null]
      - name: market
        tests: [not_null]
      - name: session_id
        tests: [not_null]

  - name: dim_equity_symbol
    description: Current state of equity metadata, SCD2-backed
    columns:
      - name: equity_id
        tests: [not_null, unique]
      - name: ticker
        tests: [not_null]
      - name: market
        tests: [not_null]
```

- [ ] **Create dim_calendar.sql:**

```sql
{{ config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(market, date)',
    partition_by='toYYYYMM(date)'
) }}

SELECT
    date,
    market,
    session_id,
    is_trading_day,
    session_open_utc,
    session_close_utc,
    toYear(date)              AS year,
    toMonth(date)             AS month,
    toWeek(date)              AS week_of_year,
    toDayOfWeek(date)         AS day_of_week
FROM {{ ref('int_calendar_sessions') }}
```

- [ ] **Create dim_equity_symbol.sql:**

```sql
SELECT
    s.equity_id,
    s.ticker,
    s.market,
    s.name,
    s.sector,
    s.industry,
    s.currency,
    s.country,
    s.market_cap,
    s.pe_ratio,
    g.sector_name             AS gics_sector,
    g.industry_group_name     AS gics_industry_group,
    g.industry_name           AS gics_industry,
    s.dbt_valid_from          AS valid_from,
    1                         AS is_current
FROM {{ ref('snap_dim_equity_symbol') }} AS s
LEFT JOIN {{ ref('gics_hierarchy') }} AS g
    ON lower(trim(s.sector)) = lower(trim(g.sector_name))
WHERE s.dbt_valid_to = '9999-12-31 00:00:00'
```

- [ ] **Run and test reference marts:**

```bash
docker compose --profile tools run --rm dadayu_dbt dbt run --select marts.reference
docker compose --profile tools run --rm dadayu_dbt dbt test --select marts.reference
```

Expected: `dim_calendar` created as table, `dim_equity_symbol` as view, all tests pass.

- [ ] **Commit:**

```bash
git add warehouse/models/marts/reference/
git commit -m "feat: add dim_calendar and dim_equity_symbol reference marts"
```

---

## Task 13: Market fact models

**Files:**
- Create: `warehouse/models/marts/markets/_schema.yml`
- Create: `warehouse/models/marts/markets/fct_ohlcv_1h.sql`
- Create: `warehouse/models/marts/markets/fct_ohlcv_4h.sql`
- Create: `warehouse/models/marts/markets/fct_ohlcv_1d.sql`

- [ ] **Create _schema.yml:**

```yaml
version: 2

models:
  - name: fct_ohlcv_1d
    description: Daily OHLCV fact enriched with dim_equity_symbol and return_pct
    columns:
      - name: ticker
        tests: [not_null]
      - name: market
        tests: [not_null]
      - name: ts
        tests: [not_null]
      - name: close
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              strictly: true
      - name: return_pct
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: -1.0
              max_value: 1.0

  - name: fct_ohlcv_1h
    description: Hourly OHLCV fact enriched with dim_equity_symbol and return_pct
    columns:
      - name: ticker
        tests: [not_null]
      - name: ts
        tests: [not_null]

  - name: fct_ohlcv_4h
    description: 4-hour OHLCV fact enriched with dim_equity_symbol and return_pct
    columns:
      - name: ticker
        tests: [not_null]
      - name: ts
        tests: [not_null]
```

- [ ] **Create fct_ohlcv_1d.sql:**

```sql
SELECT
    o.ticker,
    o.market,
    o.ts,
    o.open,
    o.high,
    o.low,
    o.close,
    o.volume,
    o.session_id,
    o.is_trading_day,
    d.name,
    d.sector,
    d.industry,
    d.currency,
    (o.close - lagInFrame(o.close) OVER w)
        / nullIf(lagInFrame(o.close) OVER w, 0)   AS return_pct
FROM {{ ref('int_equity_ohlcv_1d') }} AS o
LEFT JOIN {{ ref('dim_equity_symbol') }} AS d
    ON o.ticker = d.ticker AND o.market = d.market
WINDOW w AS (PARTITION BY o.ticker, o.market ORDER BY o.ts)
```

- [ ] **Create fct_ohlcv_4h.sql:**

```sql
SELECT
    o.ticker,
    o.market,
    o.ts,
    o.open,
    o.high,
    o.low,
    o.close,
    o.volume,
    o.session_id,
    o.is_trading_day,
    d.name,
    d.sector,
    d.industry,
    d.currency,
    (o.close - lagInFrame(o.close) OVER w)
        / nullIf(lagInFrame(o.close) OVER w, 0)   AS return_pct
FROM {{ ref('int_equity_ohlcv_4h') }} AS o
LEFT JOIN {{ ref('dim_equity_symbol') }} AS d
    ON o.ticker = d.ticker AND o.market = d.market
WINDOW w AS (PARTITION BY o.ticker, o.market ORDER BY o.ts)
```

- [ ] **Create fct_ohlcv_1h.sql:**

```sql
SELECT
    o.ticker,
    o.market,
    o.ts,
    o.open,
    o.high,
    o.low,
    o.close,
    o.volume,
    o.session_id,
    o.is_trading_day,
    d.name,
    d.sector,
    d.industry,
    d.currency,
    (o.close - lagInFrame(o.close) OVER w)
        / nullIf(lagInFrame(o.close) OVER w, 0)   AS return_pct
FROM {{ ref('int_equity_ohlcv_1h') }} AS o
LEFT JOIN {{ ref('dim_equity_symbol') }} AS d
    ON o.ticker = d.ticker AND o.market = d.market
WINDOW w AS (PARTITION BY o.ticker, o.market ORDER BY o.ts)
```

- [ ] **Run and test market facts:**

```bash
docker compose --profile tools run --rm dadayu_dbt dbt run --select marts.markets
docker compose --profile tools run --rm dadayu_dbt dbt test --select marts.markets
```

Expected: all 3 fact views created, all tests pass.

- [ ] **Commit:**

```bash
git add warehouse/models/marts/markets/
git commit -m "feat: add fct_ohlcv fact models for 1h, 4h, 1d intervals"
```

---

## Task 14: Indicator fact models

**Files:**
- Create: `warehouse/models/marts/indicators/_schema.yml`
- Create: `warehouse/models/marts/indicators/fct_indicators_1d.sql`
- Create: `warehouse/models/marts/indicators/fct_indicators_4h.sql`
- Create: `warehouse/models/marts/indicators/fct_indicators_1h.sql`

- [ ] **Create _schema.yml:**

```yaml
version: 2

models:
  - name: fct_indicators_1d
    description: Daily technical indicators (SMA, EMA, RSI, MACD, ATR, BBands)
    columns:
      - name: ticker
        tests: [not_null]
      - name: ts
        tests: [not_null]
      - name: rsi_14
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100
              row_condition: "rsi_14 IS NOT NULL"

  - name: fct_indicators_4h
    description: 4-hour technical indicators
    columns:
      - name: ticker
        tests: [not_null]
      - name: ts
        tests: [not_null]

  - name: fct_indicators_1h
    description: Hourly technical indicators
    columns:
      - name: ticker
        tests: [not_null]
      - name: ts
        tests: [not_null]
```

- [ ] **Create fct_indicators_1d.sql:**

```sql
-- Step 1: base with lag for RSI and ATR
WITH base AS (
    SELECT
        ticker,
        market,
        ts,
        open,
        high,
        low,
        close,
        lagInFrame(close) OVER (PARTITION BY ticker, market ORDER BY ts)  AS prev_close,
        close - lagInFrame(close) OVER (PARTITION BY ticker, market ORDER BY ts) AS close_diff
    FROM {{ ref('fct_ohlcv_1d') }}
),

-- Step 2: SMA, BBands, RSI, ATR — all window-function based
window_indicators AS (
    SELECT
        ticker,
        market,
        ts,
        close,
        prev_close,
        -- SMA 20
        {{ sma('close', 20) }}                          AS sma_20,
        -- BBands 20
        {{ bb_middle('close', 20) }}                    AS bb_middle,
        {{ bb_upper('close', 20, 2.0) }}                AS bb_upper,
        {{ bb_lower('close', 20, 2.0) }}                AS bb_lower,
        -- RSI 14 (requires close_diff column)
        {{ rsi(14) }}                                   AS rsi_14,
        -- ATR 14 (requires high, low, prev_close columns)
        {{ atr(14) }}                                   AS atr_14
    FROM base
),

-- Step 3: EMA and MACD via grouped arrays (ClickHouse arrayFold)
grouped AS (
    SELECT
        ticker,
        market,
        groupArray(ts)    AS ts_arr,
        groupArray(close) AS close_arr
    FROM base
    GROUP BY ticker, market
),
ema_calc AS (
    SELECT
        ticker,
        market,
        ts_arr,
        {{ ema_compute('close_arr', 20) }} AS ema20_arr,
        {{ ema_compute('close_arr', 12) }} AS ema12_arr,
        {{ ema_compute('close_arr', 26) }} AS ema26_arr
    FROM grouped
),
ema_flat AS (
    SELECT
        ticker,
        market,
        ts_arr[idx]    AS ts,
        ema20_arr[idx] AS ema_20,
        ema12_arr[idx] AS ema_12,
        ema26_arr[idx] AS ema_26
    FROM ema_calc
    ARRAY JOIN arrayEnumerate(ts_arr) AS idx
),

-- Step 4: MACD line (ema_fast - ema_slow)
macd_base AS (
    SELECT
        ticker,
        market,
        ts,
        ema_20,
        ema_12,
        ema_26,
        {{ macd_line('ema_12', 'ema_26') }}   AS macd_line
    FROM ema_flat
),
macd_with_signal AS (
    SELECT
        *,
        {{ macd_signal('macd_line', 9) }}     AS macd_signal
    FROM macd_base
)

-- Step 5: Final join
SELECT
    w.ticker,
    w.market,
    w.ts,
    w.close,
    w.sma_20,
    e.ema_20,
    w.rsi_14,
    m.macd_line,
    m.macd_signal,
    {{ macd_hist('m.macd_line', 'm.macd_signal') }}   AS macd_hist,
    w.atr_14,
    w.bb_upper,
    w.bb_middle,
    w.bb_lower
FROM window_indicators AS w
LEFT JOIN macd_with_signal AS m
    ON w.ticker = m.ticker AND w.market = m.market AND w.ts = m.ts
LEFT JOIN ema_flat AS e
    ON w.ticker = e.ticker AND w.market = e.market AND w.ts = e.ts
```

- [ ] **Create fct_indicators_4h.sql** (same pattern, different source):

```sql
WITH base AS (
    SELECT
        ticker,
        market,
        ts,
        open,
        high,
        low,
        close,
        lagInFrame(close) OVER (PARTITION BY ticker, market ORDER BY ts)  AS prev_close,
        close - lagInFrame(close) OVER (PARTITION BY ticker, market ORDER BY ts) AS close_diff
    FROM {{ ref('fct_ohlcv_4h') }}
),
window_indicators AS (
    SELECT
        ticker, market, ts, close, prev_close,
        {{ sma('close', 20) }}         AS sma_20,
        {{ bb_middle('close', 20) }}   AS bb_middle,
        {{ bb_upper('close', 20, 2.0) }}  AS bb_upper,
        {{ bb_lower('close', 20, 2.0) }}  AS bb_lower,
        {{ rsi(14) }}                  AS rsi_14,
        {{ atr(14) }}                  AS atr_14
    FROM base
),
grouped AS (
    SELECT ticker, market, groupArray(ts) AS ts_arr, groupArray(close) AS close_arr
    FROM base GROUP BY ticker, market
),
ema_calc AS (
    SELECT ticker, market, ts_arr,
           {{ ema_compute('close_arr', 20) }} AS ema20_arr,
           {{ ema_compute('close_arr', 12) }} AS ema12_arr,
           {{ ema_compute('close_arr', 26) }} AS ema26_arr
    FROM grouped
),
ema_flat AS (
    SELECT ticker, market, ts_arr[idx] AS ts,
           ema20_arr[idx] AS ema_20,
           ema12_arr[idx] AS ema_12,
           ema26_arr[idx] AS ema_26
    FROM ema_calc ARRAY JOIN arrayEnumerate(ts_arr) AS idx
),
macd_base AS (
    SELECT *, {{ macd_line('ema_12', 'ema_26') }} AS macd_line FROM ema_flat
),
macd_with_signal AS (
    SELECT *, {{ macd_signal('macd_line', 9) }} AS macd_signal FROM macd_base
)
SELECT
    w.ticker, w.market, w.ts, w.close,
    w.sma_20, e.ema_20, w.rsi_14,
    m.macd_line, m.macd_signal,
    {{ macd_hist('m.macd_line', 'm.macd_signal') }} AS macd_hist,
    w.atr_14, w.bb_upper, w.bb_middle, w.bb_lower
FROM window_indicators AS w
LEFT JOIN macd_with_signal AS m ON w.ticker=m.ticker AND w.market=m.market AND w.ts=m.ts
LEFT JOIN ema_flat AS e ON w.ticker=e.ticker AND w.market=e.market AND w.ts=e.ts
```

- [ ] **Create fct_indicators_1h.sql** (same pattern, 1h source):

```sql
WITH base AS (
    SELECT
        ticker, market, ts, open, high, low, close,
        lagInFrame(close) OVER (PARTITION BY ticker, market ORDER BY ts) AS prev_close,
        close - lagInFrame(close) OVER (PARTITION BY ticker, market ORDER BY ts) AS close_diff
    FROM {{ ref('fct_ohlcv_1h') }}
),
window_indicators AS (
    SELECT
        ticker, market, ts, close, prev_close,
        {{ sma('close', 20) }}            AS sma_20,
        {{ bb_middle('close', 20) }}      AS bb_middle,
        {{ bb_upper('close', 20, 2.0) }}  AS bb_upper,
        {{ bb_lower('close', 20, 2.0) }}  AS bb_lower,
        {{ rsi(14) }}                     AS rsi_14,
        {{ atr(14) }}                     AS atr_14
    FROM base
),
grouped AS (
    SELECT ticker, market, groupArray(ts) AS ts_arr, groupArray(close) AS close_arr
    FROM base GROUP BY ticker, market
),
ema_calc AS (
    SELECT ticker, market, ts_arr,
           {{ ema_compute('close_arr', 20) }} AS ema20_arr,
           {{ ema_compute('close_arr', 12) }} AS ema12_arr,
           {{ ema_compute('close_arr', 26) }} AS ema26_arr
    FROM grouped
),
ema_flat AS (
    SELECT ticker, market, ts_arr[idx] AS ts,
           ema20_arr[idx] AS ema_20,
           ema12_arr[idx] AS ema_12,
           ema26_arr[idx] AS ema_26
    FROM ema_calc ARRAY JOIN arrayEnumerate(ts_arr) AS idx
),
macd_base AS (
    SELECT *, {{ macd_line('ema_12', 'ema_26') }} AS macd_line FROM ema_flat
),
macd_with_signal AS (
    SELECT *, {{ macd_signal('macd_line', 9) }} AS macd_signal FROM macd_base
)
SELECT
    w.ticker, w.market, w.ts, w.close,
    w.sma_20, e.ema_20, w.rsi_14,
    m.macd_line, m.macd_signal,
    {{ macd_hist('m.macd_line', 'm.macd_signal') }} AS macd_hist,
    w.atr_14, w.bb_upper, w.bb_middle, w.bb_lower
FROM window_indicators AS w
LEFT JOIN macd_with_signal AS m ON w.ticker=m.ticker AND w.market=m.market AND w.ts=m.ts
LEFT JOIN ema_flat AS e ON w.ticker=e.ticker AND w.market=e.market AND w.ts=e.ts
```

- [ ] **Run and test indicator models:**

```bash
docker compose --profile tools run --rm dadayu_dbt dbt run --select marts.indicators
docker compose --profile tools run --rm dadayu_dbt dbt test --select marts.indicators
```

Expected: all 3 views created, RSI values between 0–100, no null ticker/ts.

- [ ] **Commit:**

```bash
git add warehouse/models/marts/indicators/
git commit -m "feat: add fct_indicators models for 1h, 4h, 1d with SMA/EMA/RSI/MACD/ATR/BBands"
```

---

## Task 15: End-to-end run and verification

- [ ] **Full dbt run from scratch:**

```bash
docker compose --profile tools run --rm dadayu_dbt dbt run
```

Expected: all models succeed, no errors.

- [ ] **Full test suite:**

```bash
docker compose --profile tools run --rm dadayu_dbt dbt test
```

Expected: all tests pass.

- [ ] **Spot-check daily indicators in ClickHouse:**

```bash
docker exec dadayu_clickhouse clickhouse-client \
  --user dadayu --password changeme --database dadayu \
  --query "
SELECT ticker, market, ts, close, sma_20, ema_20, rsi_14, macd_line, bb_upper, bb_lower
FROM fct_indicators_1d
WHERE market = 'us'
ORDER BY ticker, ts
LIMIT 10"
```

Expected: rows with non-null indicators (first 19 rows per ticker may have null sma_20 — that's correct, not enough history).

- [ ] **Spot-check dim_equity_symbol:**

```bash
docker exec dadayu_clickhouse clickhouse-client \
  --user dadayu --password changeme --database dadayu \
  --query "
SELECT market, count() AS tickers, countIf(sector != '') AS with_sector
FROM dim_equity_symbol
GROUP BY market"
```

Expected: 3 rows with tickers counts matching ingested metadata.

- [ ] **Generate dbt docs:**

```bash
docker compose --profile tools run --rm dadayu_dbt dbt docs generate
```

Expected: `target/catalog.json` created.

- [ ] **Final commit:**

```bash
git add warehouse/
git commit -m "feat: complete dbt warehouse MVP — equity OHLCV + indicators across 1h/4h/1d"
```
