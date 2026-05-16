# Crypto Ingestion & Warehouse Design

**Date:** 2026-05-16
**Status:** Approved
**Scope:** Top 20 crypto assets (by market cap, stablecoins excluded). March–May 2026, intervals 1h/4h/1d. Separate ClickHouse tables from equity pipeline.

---

## 1. Context

Equity pipeline already live: `prices_hourly/4h/daily` + `tickers` in ClickHouse, dbt warehouse with staging → intermediate → marts → indicators. Crypto extends the warehouse as a parallel, isolated subtree — same patterns, separate tables.

---

## 2. Crypto Universe

Hardcoded top 20 by market cap, stablecoins excluded (zero volatility, meaningless indicators).

| yfinance symbol | CoinGecko ID     | Category  |
|-----------------|------------------|-----------|
| BTC-USD         | bitcoin          | Layer 1   |
| ETH-USD         | ethereum         | Layer 1   |
| BNB-USD         | binancecoin      | Exchange  |
| SOL-USD         | solana           | Layer 1   |
| XRP-USD         | ripple           | Payments  |
| ADA-USD         | cardano          | Layer 1   |
| AVAX-USD        | avalanche-2      | Layer 1   |
| DOGE-USD        | dogecoin         | Meme      |
| TRX-USD         | tron             | Layer 1   |
| LINK-USD        | chainlink        | Oracle    |
| DOT-USD         | polkadot         | Layer 0   |
| MATIC-USD       | matic-network    | Layer 2   |
| LTC-USD         | litecoin         | Payments  |
| BCH-USD         | bitcoin-cash     | Payments  |
| UNI7083-USD     | uniswap          | DeFi      |
| ATOM-USD        | cosmos           | Layer 0   |
| XLM-USD         | stellar          | Payments  |
| NEAR-USD        | near             | Layer 1   |
| ICP-USD         | internet-computer| Layer 1   |
| FIL-USD         | filecoin         | Storage   |

Stored in `warehouse/seeds/crypto_universe.csv`.

---

## 3. Ingestion Scripts

### fetch_crypto_prices.py

- Args: `--interval 1h|4h|1d`, `--start YYYY-MM-DD`, `--end YYYY-MM-DD`
- Downloads via `yf.download()` for all 20 symbols, same batching pattern as `fetch_hourly_prices.py`
- Inserts into `crypto_prices_hourly` (1h), `crypto_prices_4h` (4h), `crypto_prices_daily` (1d)
- Never touches `prices_*` equity tables
- Rate: threads=True, auto_adjust=True

### fetch_crypto_info.py

- Calls CoinGecko free tier: `GET /coins/markets?vs_currency=usd&ids=<comma-list>` — one call returns all 20
- CoinGecko IDs sourced from `crypto_universe.csv` seed
- Maps response fields: `id→coin_id`, `symbol`, `name`, `market_cap_rank→rank`, `market_cap`, `categories→category` (first element), `detail_platforms→chain` (first key)
- Appends to `crypto_metadata` ClickHouse table — never truncates
- `ReplacingMergeTree(fetched_at)` handles lazy dedup; dbt reads with FINAL

### API Endpoints (api.py additions)

```
POST /run/fetch-crypto-prices?interval=1h
POST /run/fetch-crypto-prices?interval=4h
POST /run/fetch-crypto-prices?interval=1d
POST /run/fetch-crypto-info
```

Both endpoints accept optional `from_date` and `to_date` query params (default: last month).

---

## 4. ClickHouse DDL (4 new tables)

All three price tables include `market LowCardinality(String) DEFAULT 'crypto'`. This keeps indicator macros (which partition by `ticker, market`) unchanged and enables future cross-asset joins.

```sql
CREATE TABLE IF NOT EXISTS crypto_prices_hourly (
    ticker      String,
    market      LowCardinality(String) DEFAULT 'crypto',
    datetime    DateTime,
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      UInt64,
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (ticker, datetime)
PARTITION BY toYYYYMM(datetime);

CREATE TABLE IF NOT EXISTS crypto_prices_4h (
    ticker      String,
    market      LowCardinality(String) DEFAULT 'crypto',
    datetime    DateTime,
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      UInt64,
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (ticker, datetime)
PARTITION BY toYYYYMM(datetime);

CREATE TABLE IF NOT EXISTS crypto_prices_daily (
    ticker      String,
    market      LowCardinality(String) DEFAULT 'crypto',
    date        Date,
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      UInt64,
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (ticker, date)
PARTITION BY toYYYYMM(date);

CREATE TABLE IF NOT EXISTS crypto_metadata (
    coin_id     String,
    symbol      String,
    name        String,
    rank        UInt32,
    market_cap  Nullable(Float64),
    category    String,
    chain       String,
    fetched_at  DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY coin_id
PARTITION BY tuple();
```

DDL added to `db/clickhouse_init.sql`. Applied manually for existing ClickHouse volume.

---

## 5. Seeds

### crypto_universe.csv

Columns: `symbol`, `coingecko_id`, `name`, `category`
20 rows, one per asset from the universe table above.

---

## 6. dbt Layer

### Materialization

| Layer       | Materialization |
|-------------|----------------|
| staging     | view            |
| intermediate| view            |
| reference   | view            |
| markets     | view            |
| indicators  | view            |
| snapshots   | snapshot table  |

### Staging — `models/staging/coingecko/`

**`_sources.yml`** — declares `crypto_metadata` source with freshness SLA: warn after 7 days, error after 14 days.

**`stg_coingecko__crypto_info.sql`** — reads `crypto_metadata FINAL`. Renames `rank→market_rank`. Casts `market_cap` as Nullable(Float64).

### Staging — `models/staging/yahoo/` (additions)

**`stg_yahoo__crypto_ohlcv_1h.sql`** — reads `crypto_prices_hourly FINAL`. Renames `datetime→ts`, casts `volume` to UInt64.

**`stg_yahoo__crypto_ohlcv_4h.sql`** — same, reads `crypto_prices_4h FINAL`.

**`stg_yahoo__crypto_ohlcv_1d.sql`** — reads `crypto_prices_daily FINAL`. `toDateTime(date)→ts`.

**`_sources.yml`** (existing) — add `crypto_prices_hourly`, `crypto_prices_4h`, `crypto_prices_daily` sources with freshness SLAs.

### Intermediate — `models/intermediate/`

**`int_crypto_ohlcv_1h/4h/1d.sql`** — pass-through from staging. **No calendar join** — crypto trades 24/7, session metadata not applicable. Output schema: `ticker, ts, open, high, low, close, volume`.

### Snapshot — `snapshots/`

**`snap_dim_crypto_symbol.sql`** — strategy: `check`, check_cols: `['rank', 'market_cap', 'category']`. Unique key: `coin_id`. Captures rank changes and market cap movements as SCD2 history.

### Marts — `models/marts/reference/`

**`dim_crypto_symbol.sql`** — reads `snap_dim_crypto_symbol WHERE dbt_valid_to IS NULL`. Left-joins `crypto_universe` seed on `symbol` to add `coingecko_id`. Columns: `coin_id`, `symbol`, `name`, `market_rank`, `market_cap`, `category`, `chain`, `coingecko_id`.

### Marts — `models/marts/markets/`

**`fct_ohlcv_crypto_1h/4h/1d.sql`** — reads `int_crypto_ohlcv_*`. Left-joins `dim_crypto_symbol` on `ticker = symbol` for `name`, `category`, `market_rank`. Adds `return_pct = (close - lag(close)) / lag(close)` via `lagInFrame` window function partitioned by `ticker` ordered by `ts`. No session columns.

### Marts — `models/marts/indicators/`

**`fct_indicators_crypto_1h/4h/1d.sql`** — identical CTE pattern to equity indicator models. Same 6 macros: `sma(20)`, `ema(20)`, `rsi(14)`, `macd(12/26/9)`, `atr(14)`, `bbands(20)`. Window partition by `ticker` only (no `market` column).

Output columns: `ticker, ts, close, sma_20, ema_20, rsi_14, macd_line, macd_signal, macd_hist, atr_14, bb_upper, bb_middle, bb_lower`.

---

## 7. dbt_project.yml additions

```yaml
models:
  dadayu_warehouse:
    staging:
      coingecko:
        +materialized: view

seeds:
  dadayu_warehouse:
    crypto_universe:
      +column_types:
        symbol: String
        coingecko_id: String
```

---

## 8. Execution Order

```bash
# 1. Create tables (one-time)
docker exec dadayu_clickhouse clickhouse-client ... < db/clickhouse_init.sql

# 2. Fetch prices (March–May 2026)
curl -X POST "http://localhost:8000/run/fetch-crypto-prices?interval=1d&from_date=2026-03-01&to_date=2026-05-16"
curl -X POST "http://localhost:8000/run/fetch-crypto-prices?interval=4h&from_date=2026-03-01&to_date=2026-05-16"
curl -X POST "http://localhost:8000/run/fetch-crypto-prices?interval=1h&from_date=2026-03-01&to_date=2026-05-16"

# 3. Fetch metadata
curl -X POST "http://localhost:8000/run/fetch-crypto-info"

# 4. dbt
docker compose run dadayu_dbt seed --select crypto_universe
docker compose run dadayu_dbt snapshot
docker compose run dadayu_dbt run --select staging.coingecko staging.yahoo intermediate marts
docker compose run dadayu_dbt test
```

---

## 9. Deferred

- Funding rates / open interest (requires exchange API — Binance, Bybit)
- On-chain volume (requires CoinGecko Pro or Dune)
- `obt_unified_wide` cross-asset table (Polymarket spec)
- `map_pm_to_underlying` moat (Polymarket spec)
- Crypto 1h indicators for full 1h dataset (benchmark query time first)
