# Polymarket Integration Design

**Date:** 2026-05-20
**Status:** Approved

## Goal

Add Polymarket prediction market data to the DADAYU stack. Ingest market metadata and hourly/daily probability time series. Model probability signals in dbt alongside existing OHLCV and indicator marts. Enable correlation analysis between prediction market probabilities and price movements across crypto, equity, and macro.

---

## Section 1: Architecture

```
Gamma API ─────► polymarket_markets (raw) ─► stg_polymarket__markets
                                                        │
CLOB API ──────► polymarket_prices (raw) ──► stg_polymarket__prices
                                                        │
                                              int_polymarket_prices_1h
                                              int_polymarket_prices_1d
                                              (resample + forward-fill)
                                                        │
                                              fct_polymarket_signals
                                              (Δp, log_odds, volume,
                                               linked_asset, is_interpolated)
```

**New Dagster assets:**
- `polymarket_markets` — daily, discovers all active markets with volume > $50k via Gamma API
- `polymarket_prices` — every 4 hours, fetches CLOB price history for all active markets (watermarked per market)

**New job:** `polymarket_job = polymarket_markets + polymarket_prices + dadayu_dbt_assets`

**New schedule:** every 4 hours (crypto-like cadence — prediction markets never close)

**New ingest module:** `dadayu/ingest/polymarket.py` — follows same structure as `dadayu/ingest/crypto.py`

**Manual override seed:** `warehouse/seeds/polymarket_asset_map.csv` — `condition_id, linked_asset, asset_type` for markets where auto-parse misses the ticker

---

## Section 2: ClickHouse Raw Schema

Add to `db/clickhouse_init.sql`:

```sql
-- Polymarket market metadata — upserted daily
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
    outcome           Nullable(String),    -- 'YES' | 'NO' | null
    yes_token_id      String,              -- CLOB token ID for price history calls
    linked_asset      Nullable(String),    -- e.g. 'BTC-USD', 'AAPL', null
    asset_type        Nullable(String),    -- 'crypto' | 'equity' | 'macro' | null
    fetched_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY condition_id;

-- Polymarket hourly probability snapshots
CREATE TABLE IF NOT EXISTS polymarket_prices
(
    condition_id  String,
    ts            DateTime,
    probability   Float64,    -- YES token price = implied probability [0, 1]
    volume_usd    Float64,    -- USD volume traded in this candle
    ingested_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (condition_id, ts)
PARTITION BY toYYYYMM(ts);
```

**Key design note:** `yes_token_id` is stored separately from `condition_id`. The Gamma API returns both. The CLOB price-history endpoint requires the token ID, not the market/condition ID — this is the primary Polymarket API gotcha.

---

## Section 3: Ingestion (`dadayu/ingest/polymarket.py`)

### APIs

| API | Base URL | Auth |
|---|---|---|
| Gamma (metadata) | `https://gamma-api.polymarket.com` | None |
| CLOB (prices) | `https://clob.polymarket.com` | None (read-only) |

### Functions

**`discover_markets(min_volume_usd=50000) -> pd.DataFrame`**

```
GET https://gamma-api.polymarket.com/markets
  ?active=true&closed=false&volume_num_min={min_volume_usd}&limit=500
```

Returns: `condition_id, question, category, volume_usd, liquidity_usd, active, closed, resolution_date, outcome, yes_token_id`

Auto-parses `linked_asset` by scanning question text for known tickers from `crypto_universe.csv`. Manual overrides from `polymarket_asset_map.csv` take precedence.

**`fetch_price_history(yes_token_id, start_ts, end_ts) -> pd.DataFrame`**

```
GET https://clob.polymarket.com/prices-history
  ?market_id={yes_token_id}&startTs={unix}&endTs={unix}&fidelity=60
```

`fidelity=60` = 1h candles. Returns: `ts, probability, volume_usd`.

**`fetch_daily_price_history(yes_token_id, start_ts, end_ts) -> pd.DataFrame`**

Same endpoint, `fidelity=1440` for daily candles.

### Rate Limiting Strategy

- `time.sleep(0.15)` between CLOB calls (~6 req/s sustained)
- HTTP 429 → exponential backoff, up to 3 retries, max wait 60s
- Watermark **per `condition_id`**: each market tracks its own `max(ts)` via `get_watermark(client, "polymarket_prices", "ts", condition_id=id)`. First run backfills 90 days. Subsequent runs fetch delta only (~1-4h of data per market = tiny payload).
- Skip closed markets entirely: Dagster asset queries `polymarket_markets WHERE closed = false` before fetching prices. Resolved markets permanently exit the fetch loop.
- Discovery runs daily (not every 4h) — keeps discovery cost decoupled from pricing cost.

### Watermark extension

`dadayu/watermark.py` `get_watermark()` needs a `condition_id` parameter to support per-market watermarking:

```python
get_watermark(client, "polymarket_prices", "ts", condition_id="0xabc...")
# Adds: WHERE condition_id = {condition_id:String}
```

---

## Section 4: dbt Layer

### Staging (`warehouse/models/staging/polymarket/`)

- `stg_polymarket__markets.sql` — cast types, coalesce nulls on nullable fields, expose `linked_asset` and `asset_type`
- `stg_polymarket__prices.sql` — filter `probability BETWEEN 0 AND 1`, cast `ts`, dedupe via `FINAL` (ClickHouse ReplacingMergeTree)

### Intermediate (`warehouse/models/intermediate/`)

- `int_polymarket_prices_1h.sql` — resample to 1h:
  - `toStartOfHour(ts)` bucket
  - `argMax(probability, ts)` = close-of-hour probability
  - `sum(volume_usd)` = total volume in bucket
  - `is_interpolated = (count() = 0)` — true when no trade occurred in that bucket (gap hours forward-filled from previous row using `lagInFrame`)

- `int_polymarket_prices_1d.sql` — same, `toStartOfDay(ts)` + `argMax`

### Mart (`warehouse/models/marts/`)

**`fct_polymarket_signals.sql`** — one row per (condition_id, ts):

| Column | Type | Description |
|---|---|---|
| `condition_id` | String | Market identifier |
| `ts` | DateTime | 1h or 1d bucket start |
| `probability` | Float64 | Close-of-period implied probability [0, 1] |
| `prob_change` | Float64 | `probability - lag(probability)` — primary signal (Δp) |
| `log_odds` | Float64 | `ln(probability / (1 - probability))`, clipped at [0.01, 0.99] |
| `volume_usd` | Float64 | Trading volume in bucket |
| `is_interpolated` | Bool | True if no trade in this bucket (forward-filled) |
| `question` | String | Market question text |
| `linked_asset` | String (nullable) | Ticker for join: `BTC-USD`, `AAPL`, null |
| `asset_type` | String (nullable) | `crypto` / `equity` / `macro` / null |
| `days_to_resolution` | Int32 | `dateDiff('day', ts, resolution_date)` |

**Correlation join (future mart — not in scope now):**
`fct_polymarket_signals` LEFT JOIN `fct_ohlcv_crypto_1h` / `fct_ohlcv_1h` ON `linked_asset = ticker AND ts = ts`.

### Snapshot

`warehouse/snapshots/snap_dim_polymarket_market.sql` — SCD Type 2 on `polymarket_markets`. Retains historical metadata for resolved/delisted markets (survivorship bias mitigation).

---

## Section 5: Modelling Challenges

### 1. Bounded probability vs unbounded returns
`prob_change` (Δp) is bounded `[-1, 1]`. Price returns are unbounded. Pearson correlation is valid but use `log_odds` for regression. Clip probability to `[0.01, 0.99]` before computing log-odds to avoid ±∞ at resolution.

### 2. Near-expiry collapse
In the last 24-48h before resolution, probability collapses to 0 or 1, producing massive Δp spikes. `days_to_resolution` column lets analysts filter `WHERE days_to_resolution > 2` to exclude expiry distortion from correlation windows.

### 3. Forward-fill creates false stationarity
Hours with no trades carry forward last probability. A flat Δp=0 in a no-trade hour is not information — it's silence. `is_interpolated = true` flags these rows so they can be excluded or down-weighted in correlation analysis.

### 4. Market-asset linkage is noisy
Crypto price markets (`"Will BTC exceed $100k?"`) reliably auto-parse to a ticker. Macro markets (`"Will the US avoid a recession?"`) have no single linked asset — `linked_asset = null`, `asset_type = 'macro'`. These are still useful as broad market direction signals; `polymarket_asset_map.csv` provides manual overrides for ambiguous cases.

### 5. Survivorship bias
Delisted or disputed markets disappear from Gamma API. `snap_dim_polymarket_market` retains historical market metadata via SCD Type 2 snapshot, ensuring resolved/delisted markets remain in the historical dataset for unbiased analysis.

### 6. Lead/lag analysis (future scope)
Cross-correlation at multiple lags (Δp at t vs return at t+1, t+2, t+6, t+24h) is deferred to a future mart. Current design stores all the inputs needed — `is_interpolated`, `days_to_resolution`, `prob_change` — to support this without schema changes.

---

## File Changes

**New files:**
- `dadayu/ingest/polymarket.py`
- `dagster_pipeline/assets/polymarket.py`
- `warehouse/models/staging/polymarket/_sources.yml`
- `warehouse/models/staging/polymarket/stg_polymarket__markets.sql`
- `warehouse/models/staging/polymarket/stg_polymarket__markets.yml`
- `warehouse/models/staging/polymarket/stg_polymarket__prices.sql`
- `warehouse/models/staging/polymarket/stg_polymarket__prices.yml`
- `warehouse/models/intermediate/int_polymarket_prices_1h.sql`
- `warehouse/models/intermediate/int_polymarket_prices_1d.sql`
- `warehouse/models/marts/markets/fct_polymarket_signals.sql`
- `warehouse/models/marts/markets/fct_polymarket_signals.yml`
- `warehouse/snapshots/snap_dim_polymarket_market.sql`
- `warehouse/seeds/polymarket_asset_map.csv`

**Modified files:**
- `db/clickhouse_init.sql` — add two new tables
- `dadayu/watermark.py` — add `condition_id` param support
- `dagster_pipeline/definitions.py` — add polymarket assets + job + schedule
- `dagster_pipeline/schedules.py` — add polymarket_job + polymarket_schedule

**Untouched:**
- All existing equity/crypto assets, dbt models, ClickHouse tables
