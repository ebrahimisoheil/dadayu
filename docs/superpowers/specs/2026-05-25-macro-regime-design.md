# Macro Regime & Cross-Asset Analytics — Design Spec

**Date:** 2026-05-25
**Status:** Approved for implementation

---

## Goals

1. Ingest 25 macro/cross-asset tickers (ETFs, futures, rate indices) via yfinance
2. Replace the current single-factor S&P regime with a multi-dimensional composite macro regime (continuous 0–100 score + 5-state label)
3. Expose a Metabase-facing mart for cross-asset analytics dashboards
4. Keep informational only — not wired to backtest exposure multiplier

---

## Tickers

| Ticker | Name | Instrument | Dimension |
|---|---|---|---|
| HYG | iShares HY Corporate Bond | etf | credit |
| LQD | iShares IG Corporate Bond | etf | credit |
| TLT | iShares 20+ Year Treasury | etf | rates |
| IEF | iShares 7-10 Year Treasury | etf | rates |
| SHY | iShares 1-3 Year Treasury | etf | rates |
| TIP | iShares TIPS Bond | etf | inflation |
| ^TNX | 10-Year Treasury Yield | rate_index | rates |
| GLD | SPDR Gold Shares | etf | inflation |
| SLV | iShares Silver Trust | etf | inflation |
| USO | United States Oil Fund | etf | inflation |
| GC=F | Gold Futures | future | inflation |
| SI=F | Silver Futures | future | inflation |
| CL=F | Crude Oil Futures | future | inflation |
| CPER | United States Copper Index | etf | growth |
| DBB | Invesco DB Base Metals | etf | growth |
| HG=F | Copper Futures | future | growth |
| EFA | iShares MSCI EAFE | etf | growth |
| EEM | iShares MSCI Emerging Markets | etf | growth |
| UUP | Invesco DB US Dollar Index | etf | dollar |
| VNQ | Vanguard Real Estate ETF | etf | rates |
| XLE | Energy Select Sector SPDR | etf | sector |
| XLF | Financial Select Sector SPDR | etf | sector |
| XLK | Technology Select Sector SPDR | etf | sector |
| XLV | Health Care Select Sector SPDR | etf | sector |
| XLI | Industrial Select Sector SPDR | etf | sector |
| XLU | Utilities Select Sector SPDR | etf | sector |

---

## Architecture

```
macro_universe.csv (seed)
    ↓
macro_prices_daily (ClickHouse table)
    ↓ macro_ohlcv (Dagster asset, weekdays 22:15 UTC)
stg_yahoo__macro_ohlcv_daily (staging)
    ↓
int_macro_assets_daily (intermediate: indicators per ticker)
    ↓
int_macro_regime_daily (replaces int_market_regime_daily)
    ↓
mart_macro_regime_daily (Metabase-facing)
```

---

## 1. Seed — `warehouse/seeds/macro_universe.csv`

Columns: `macro_id, ticker, market, name, instrument_type, regime_dimension`

- `market` = `"macro"` for all rows (consistent with existing market taxonomy)
- `instrument_type` ∈ `{etf, future, rate_index}`
- `regime_dimension` ∈ `{credit, rates, inflation, dollar, growth, sector}`

---

## 2. ClickHouse Table — `macro_prices_daily`

Identical schema to `index_prices_daily`. Add `CREATE TABLE IF NOT EXISTS` block to `db/clickhouse_init.sql`.

Schema: `ticker, market, ts, open, high, low, close, volume` with MergeTree engine, `ORDER BY (ticker, market, ts)`, `PARTITION BY toYYYYMM(ts)`.

---

## 3. Ingestion — `dadayu/ingest/macro.py`

Structurally identical to `dadayu/ingest/indexes.py`:
- Reads `warehouse/seeds/macro_universe.csv`
- Exposes `load_symbols()`, `load_universe()`
- `MARKET = "macro"`, `INTERVAL_TABLE = {"1d": "macro_prices_daily"}`
- Reuses `download_ohlcv` from `dadayu/ingest/equity.py`

---

## 4. Dagster Asset — `dagster_pipeline/assets/macro.py`

Identical pattern to `dagster_pipeline/assets/indexes.py`:
- Asset name: `macro_ohlcv`
- Group: `ingestion`
- 5-year backfill on first run, watermark-driven incremental after

**Schedule additions in `schedules.py`:**
```python
macro_job = define_asset_job(
    name="macro_job",
    selection=AssetSelection.assets(macro_ohlcv, *_DBT_DAILY_ASSETS),
)
macro_schedule = ScheduleDefinition(
    job=macro_job,
    cron_schedule="15 22 * * 1-5",  # 15 min after equity
)
```

Add `macro_ohlcv` to `dagster_pipeline/assets/__init__.py` exports.

---

## 5. Staging — `stg_yahoo__macro_ohlcv_daily.sql`

Path: `warehouse/models/01_staging/yahoo/stg_yahoo__macro_ohlcv_daily.sql`

- Source: `macro_prices_daily`
- Left join `macro_universe` seed on `ticker` to attach `instrument_type`, `regime_dimension`, `name`
- Standard type casting, null guard on close/volume
- `materialized='incremental'`, `unique_key=['ticker', 'ts']`

---

## 6. Intermediate — `int_macro_assets_daily.sql`

Path: `warehouse/models/02_intermediate/macro/int_macro_assets_daily.sql`

Computed per `(ticker, ts)`:
- `return_pct` — daily return
- `return_20d_pct`, `return_60d_pct` — rolling returns via window functions
- `sma_20`, `sma_50`, `sma_200` — simple moving averages
- `above_sma_20`, `above_sma_50`, `above_sma_200` — boolean flags
- `pct_rank_return_20d` — percentile rank of 20d return in trailing 252-day window (0–1), used for normalized scoring

`materialized='table'`, partitioned by month.

---

## 7. Regime — `int_macro_regime_daily.sql`

Path: `warehouse/models/02_intermediate/market/int_macro_regime_daily.sql`

Replaces `int_market_regime_daily.sql`. The old file is updated to `SELECT * FROM {{ ref('int_macro_regime_daily') }}` for backward compatibility (no downstream models break).

### Sub-scores (each 0–100, higher = more risk-on)

**credit_score**
- HYG 20d return > 0: +40
- HYG above SMA50: +10
- LQD 20d return > 0: +40
- LQD above SMA50: +10

**rates_score** (combines yield direction via ^TNX with TLT flight-to-quality signal, confirmed by credit context)
- `^TNX` 20d return < 0 (yields falling = rate relief): +30
- `^TNX` 20d return > 1% (yields rising fast = rate pressure): −20
- TLT 20d return > 2% AND credit_score >= 60 (bond rally in healthy credit env = benign easing): +20
- TLT 20d return > 2% AND credit_score < 40 (bond rally + credit stress = flight to safety): −30
- VNQ above SMA50 (rate-sensitive assets stable): +10
- Base: 40
- Clamp to [0, 100]
- Note: `^TNX` stores yield as price (4.5 = 4.5%). credit_score is computed first and passed as input to rates_score. TLT alone is ambiguous (falling yields can be growth-bullish or fear-driven); credit confirmation disambiguates.

**inflation_score** (rising inflation = ambiguous, scored as drag on equity)
- GLD 20d return > 3% AND CL=F 20d return > 3%: −40 (inflation spike = risk-off)
- TIP above SMA50: −20 (inflation expectations elevated)
- GLD 20d return < 0 AND CL=F 20d return < 0: +30 (disinflation = equity-friendly)
- Base: 60
- Clamp to [0, 100]

**dollar_score** (strong dollar = risk-off for equities, commodities, EM)
- UUP above SMA20: −40
- UUP above SMA50: −20
- UUP 20d return > 2%: −20 (sharp dollar rally = risk-off)
- Base: 80
- Clamp to [0, 100]

**growth_score** (industrial metals + EM = global growth proxy)
- DBB above SMA50: +20
- CPER above SMA50: +20
- HG=F 20d return > 0: +15
- EEM above SMA50: +20
- EFA above SMA50: +15
- Base: 10
- Clamp to [0, 100]

**sector_score** (cyclicals vs defensives rotation)
- XLK above SMA50: +15
- XLF above SMA50: +15
- XLI above SMA50: +15
- XLE above SMA50: +10
- XLU above SMA50: −20 (defensive leading = risk-off)
- XLV above SMA50: −15 (healthcare leading = defensive)
- Base: 30
- Clamp to [0, 100]

### Composite Score

```
composite_macro_score = (
    credit_score    × 0.25
  + growth_score    × 0.25
  + dollar_score    × 0.15
  + sector_score    × 0.15
  + rates_score     × 0.10
  + inflation_score × 0.10
)
```

Weights reflect: credit and growth are strongest leading indicators. Rates are included but down-weighted because rising rates can be both growth-bullish (expansion) and equity-bearish (tightening). Inflation similarly ambiguous — present enough to register commodity spikes, not enough to dominate.

Weights sum to 1.00.

### 5-State Dashboard Labels

```
composite >= 80 → risk_on
composite >= 60 → constructive
composite >= 40 → neutral
composite >= 20 → defensive
composite <  20 → risk_off
```

### Hysteresis Bands (Strategy Gates)

Dashboard labels use fixed thresholds above. For any future strategy logic using this regime, apply wider confirmation bands to reduce flip frequency:

```
enter risk_on:   score >= 70 (was constructive, now confirmed risk_on)
exit risk_on:    score <  55
enter risk_off:  score <= 30 (was defensive, now confirmed risk_off)
exit risk_off:   score >  45
```

These bands are stored as constants in `mart_macro_regime_daily` (columns: `risk_on_entry_threshold=70`, `risk_on_exit_threshold=55`, `risk_off_entry_threshold=30`, `risk_off_exit_threshold=45`) so downstream consumers don't hardcode them.

### Output columns

`ts, credit_score, rates_score, inflation_score, dollar_score, growth_score, sector_score, composite_macro_score, macro_regime (5-state), benchmark_close, benchmark_return_pct, benchmark_above_sma_200, risk_on_score (legacy compat field)`

The legacy `market_regime` (3-state) and `risk_on_score` fields are populated from the new 5-state mapping for backward compatibility:
```
risk_on / constructive → risk_on (legacy)
neutral               → neutral (legacy)
defensive / risk_off  → risk_off (legacy)
```

---

## 8. Mart — `mart_macro_regime_daily.sql`

Path: `warehouse/models/03_marts/macro/mart_macro_regime_daily.sql`

Metabase-facing, one row per date. Adds:
- `composite_macro_score_30d_avg` — 30-day rolling average of composite (smoothed trend)
- `composite_macro_score_trend` — `composite_macro_score - composite_macro_score_30d_avg` (momentum of regime)
- All 6 dimension scores
- `macro_regime` (5-state), `ts`

`materialized='table'`, `order_by='ts'`

---

## 9. Schema & Tests

Add to `warehouse/models/02_intermediate/schema.yml`:
- `int_macro_assets_daily`: uniqueness test on `(ticker, ts)`
- `int_macro_regime_daily`: not_null on `ts`, `composite_macro_score`, `macro_regime`; accepted_values for `macro_regime`

Add `warehouse/models/03_marts/macro/schema.yml`:
- `mart_macro_regime_daily`: uniqueness on `ts`, not_null on all score columns

---

## 10. Dagster Definitions Update

`dagster_pipeline/definitions.py`: add `macro_ohlcv` asset and `macro_schedule` to `Definitions`.

---

## Out of Scope

- Wiring `composite_macro_score` to backtest `exposure_multiplier` (deferred)
- Intraday or hourly macro data
- Futures roll/expiry adjustment (yfinance continuous contracts used as-is)
- Real-time macro data feeds

---

## File Checklist

```
NEW  warehouse/seeds/macro_universe.csv
NEW  dadayu/ingest/macro.py
NEW  dagster_pipeline/assets/macro.py
NEW  warehouse/models/01_staging/yahoo/stg_yahoo__macro_ohlcv_daily.sql
NEW  warehouse/models/02_intermediate/macro/int_macro_assets_daily.sql
NEW  warehouse/models/02_intermediate/market/int_macro_regime_daily.sql
NEW  warehouse/models/03_marts/macro/mart_macro_regime_daily.sql
NEW  warehouse/models/03_marts/macro/schema.yml
MOD  warehouse/models/02_intermediate/market/int_market_regime_daily.sql  (→ passthrough ref)
MOD  db/clickhouse_init.sql  (add macro_prices_daily table)
MOD  dagster_pipeline/assets/__init__.py  (export macro_ohlcv)
MOD  dagster_pipeline/schedules.py  (add macro_job, macro_schedule)
MOD  dagster_pipeline/definitions.py  (register macro asset + schedule)
MOD  warehouse/models/02_intermediate/schema.yml  (add macro model docs/tests)
```
