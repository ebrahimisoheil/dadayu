# DADAYU Data Layer — Reference Guide

> Everything in this document reflects live data as of **2026-05-28**.
> Tech stack: Python 3.12 · yfinance · Postgres 16 · dbt · Dagster.

---

## What We Have in One Sentence

Daily OHLCV price history for **618 stocks** (US + Germany) and **26 macro instruments**, enriched with momentum scores, sector/industry rankings, a 6-dimension macro regime score, and 48 backtested momentum strategies — all pre-joined into 4 cadence-specific OBT marts (weekly / monthly / quarterly / 6-month) ready to query with zero joins.

---

## Universe

| Market | Stocks | With Sector | With Industry |
|--------|--------|-------------|---------------|
| US (S&P 500 + extras) | 531 | 530 | 530 |
| Germany (DAX / MDAX / SDAX) | 88 | 87 | 87 |
| **Total** | **619** | **617** | **617** |

**Macro instruments (26):** ETFs, rate indices, and futures covering credit (HYG, LQD), rates (TLT, IEF, SHY, TIP, ^TNX), inflation (GLD, SLV, USO, GC=F, SI=F, CL=F), growth (CPER, DBB, HG=F, EEM, EFA), dollar (UUP), real estate (VNQ), and 6 US sector ETFs (XLE, XLF, XLK, XLV, XLI, XLU).

**Benchmark indexes (6):** S&P 500, NASDAQ 100, Russell 2000, DAX 40, MDAX, SDAX.

---

## Index Universe History & Membership

The universe is anchored in two committed seed CSVs (DE backfill from STOXX PDF; US backfill from GitHub SP500 dataset) and tracked forward via a dbt SCD2 snapshot of live-scraped membership. All three layers are merged by `int_universe_membership_daily` into non-overlapping point-in-time spans.

### Architecture

**Three layers:**

1. **Backfill seeds** (committed CSVs, hand-reconciled — source of truth)
   - Germany: `warehouse/seeds/seed_index_membership_de.csv` (resolved; covers DAX, MDAX, SDAX, TecDAX)
   - Germany raw: `warehouse/seeds/seed_index_membership_de_raw.csv` (PDF parser output, input to resolver)
   - Germany crosswalk: `warehouse/seeds/seed_index_name_ticker_map.csv` (company name → .DE ticker)
   - US: `warehouse/seeds/seed_index_membership_us.csv` (SP500 spans back to 1996)

2. **Live SCD2 snapshot** (forward from go-live)
   - `warehouse/snapshots/snap_index_membership.sql` — dbt snapshot of `stg_membership__observed`
   - Each Dagster ingest run writes current index membership to `index_membership_observed`; dbt snapshot closes spans when a ticker leaves

3. **Unified model**
   - `int_universe_membership_daily` — merges seeds + snapshot via island-and-gap, producing one non-overlapping span set per `(ticker, market)`

### Valid Span Convention

- `valid_from` — **inclusive** (first day stock is in index)
- `valid_to` — **exclusive** (first day stock is NOT in index); NULL = still active
- Applies to all three layers

### Point-in-Time Coverage

**Best-effort, asymmetric by market:**

| Index | Coverage | Notes |
|-------|----------|-------|
| DAX | ~1988–present | STOXX PDF has detailed historical compositions; pre-2010 defunct names excluded (no yfinance data) |
| MDAX / SDAX / TecDAX | ~2003–present | STOXX PDF less granular before 2020; some gaps in delisted tickers |
| SP500 | ~1996–present | GitHub dataset (fja05680/sp500); pre-2010 coverage best-effort |

**Pre-2010 defunct names** are excluded from both backfills — yfinance cannot fetch data for delisted tickers; those tickers would produce empty OHLCV tables.

### Membership Floor Thresholds

Enforced by dbt singular tests (`warehouse/tests/universe_active_floor_de.sql`, `universe_active_floor_us.sql`) and Python `checks.py::check_universe_membership()`.

| Market | Min Active Members | Rationale |
|--------|-------------------|-----------|
| Germany (DAX + MDAX + SDAX + TecDAX) | ≥ 120 | Typical: ~139+ members |
| US (SP500) | ≥ 450 | Typical: ~500 members; threshold accounts for turnover |

**Bootstrap note:** On a fresh clone with no prior Dagster run, the DE floor is met by seeds alone (139 active). The US floor is met by `seed_index_membership_us.csv` (503 active). The SCD2 snapshot starts empty and fills after the first `equity_index_membership` asset run.

### Refreshing Seeds

Seeds are committed CSVs. To regenerate after updating source files (STOXX PDF, GitHub data):

```bash
# Germany: parse STOXX PDF → raw seed (manual review required before committing)
curl -sL -A "Mozilla/5.0" -o /tmp/stoxx.pdf \
  "https://www.stoxx.com/document/Indices/Common/Indexguide/Historical_Index_Compositions.pdf"
python3 tools/parse_stoxx_compositions.py /tmp/stoxx.pdf warehouse/seeds/seed_index_membership_de_raw.csv

# Germany: resolve names → tickers using crosswalk
python3 tools/resolve_de_membership.py \
  warehouse/seeds/seed_index_membership_de_raw.csv \
  warehouse/seeds/seed_index_name_ticker_map.csv \
  warehouse/seeds/seed_index_membership_de.csv

# US: rebuild from GitHub changes CSV + current constituents
# (download changes.csv and current.txt from fja05680/sp500, then:)
python3 tools/build_us_membership.py changes.csv current.txt 2026-06-28 \
  warehouse/seeds/seed_index_membership_us.csv

# Commit updated CSVs
git add warehouse/seeds/seed_index_membership_de_raw.csv \
        warehouse/seeds/seed_index_membership_de.csv \
        warehouse/seeds/seed_index_name_ticker_map.csv \
        warehouse/seeds/seed_index_membership_us.csv
git commit -m "refactor: refresh membership seeds from source [DATE]"

# Reload seeds + rebuild affected models
cd warehouse && dbt seed && dbt run --select int_universe_membership_daily && dbt test
```

### Membership Tables

| Table | Grain | Purpose |
|-------|-------|---------|
| `seed_index_membership_de` | ticker × index × valid_from | DE membership spans from STOXX PDF (resolved) |
| `seed_index_membership_us` | ticker × valid_from | US SP500 spans from GitHub dataset |
| `snap_index_membership` | ticker × market × dbt_valid_from | SCD2 snapshot of live-scraped membership |
| `int_universe_membership_daily` | ticker × market × valid_from | Merged, non-overlapping spans (query here) |

---

## Raw Price Data

### `prices_daily` — Equity OHLCV
Primary price table for all 618 stocks.

| Column | Type | Description |
|--------|------|-------------|
| ticker | text | Yahoo Finance symbol (e.g. AAPL, SAP.DE) |
| market | text | `us` or `germany` |
| date | date | Trading day |
| open / high / low / close | double | Adjusted prices |
| volume | bigint | Daily volume |
| ingested_at | timestamp | When row was written |

**Coverage:** 771,947 rows · 618 tickers · 2021-05-28 → 2026-05-28 · ~5 years

### `index_prices_daily` — Benchmark Indexes
Same schema as `prices_daily`. 7,551 rows · 6 indexes.

### `macro_prices_daily` — Macro Instruments
Same schema as `prices_daily`, `market = 'macro'`. 32,639 rows · 26 instruments · 2021-05-28 → 2026-05-28.

### `tickers` — Equity Metadata
| Column | Type | Description |
|--------|------|-------------|
| ticker | text | Symbol |
| market | text | `us` or `germany` |
| name | text | Company name |
| sector | text | GICS sector |
| industry | text | GICS industry |
| market_cap | double | Latest market cap (USD) |

619 rows. 2 tickers missing sector (data gap from yfinance).

---

## Seeds (Reference Tables)

| Seed | Rows | Purpose |
|------|------|---------|
| `macro_universe` | 26 | Macro instrument definitions — ticker, market, name, instrument_type, regime_dimension |
| `index_universe` | 6 | Benchmark index definitions |
| `gics_hierarchy` | ~160 | Full GICS sector → industry mapping |
| `equity_universe_extra` | varies | Extra tickers added beyond standard indexes |

---

## dbt Pipeline — Layer by Layer

### Layer 1: Staging (`01_staging`)

Cleans raw Postgres tables, adds type casts, joins seed metadata.

| Model | Source | Rows | Key additions |
|-------|--------|------|---------------|
| `stg_yahoo__equity_ohlcv_daily` | prices_daily | ~772k | market validation, close > 0 filter |
| `stg_yahoo__index_ohlcv_daily` | index_prices_daily | ~7.5k | index_id from seed |
| `stg_yahoo__macro_ohlcv_daily` | macro_prices_daily | ~32.6k | name, instrument_type, regime_dimension from macro_universe seed |
| `stg_yahoo__ticker_info` | tickers | ~619 | cleaned metadata |

### Layer 2: Intermediate (`02_intermediate`)

Heavy computation — indicators, scores, quality flags, regime.

| Model | Rows | Purpose |
|-------|------|---------|
| `int_market_asset_indicators_daily` | ~240MB | Per-ticker daily: RSI, MACD, Bollinger, ATR, CMO, EMA, SMA 20/50/200 |
| `int_market_assets_daily` | ~128MB | Indicators + momentum 12-1, rankability flags, data quality flags |
| `int_market_data_quality_daily` | ~111MB | Per-row quality flags: invalid_ohlc, extreme_return, stale_price, low_liquidity |
| `int_market_forward_prices_daily` | ~55MB | Forward returns at 5d/10d/20d/60d per ticker — used by backtest engine |
| `int_market_backtest_prices_daily` | ~111MB | Entry/exit prices for backtest simulation |
| `int_backtest_asset_scores_daily` | ~97MB | Scores computed on rebalance dates for all 48 strategies |
| `int_market_indexes_daily` | ~1.3MB | Index OHLCV + SMA20/50/200 + returns |
| `int_market_regime_daily` | ~232KB | Passthrough → `int_macro_regime_daily` (legacy compat) |
| `int_macro_assets_daily` | ~4.4MB | Per macro ticker: 1d/20d/60d returns, SMA20/50/200, above_sma flags |
| `int_macro_regime_daily` | ~232KB | 6-dimension composite regime score (see Macro Regime section) |
| `int_reference_equity_symbols_current` | ~136KB | Latest known ticker metadata snapshot |

### Layer 3: Marts (`03_marts`)

Pre-aggregated, product-ready tables. Zero joins needed at query time.

---

## Mart Reference

### Portfolio Scoring

#### `mart_portfolio_asset_scores_daily`
One row per (ticker, market, score_date). The core scoring model.

| Column | Type | Description |
|--------|------|-------------|
| ticker / market / asset_type | text | Identity |
| score_date | timestamp | Date of score |
| name / sector / industry | text | Metadata |
| close / sma_20 / sma_50 / sma_200 | double | Price levels |
| rsi_14 | double | RSI (14-day) |
| macd_hist | double | MACD histogram |
| cmo_10 | double | Chande Momentum Oscillator (10-day) |
| cmo_monthly_10 | double | CMO on monthly bars |
| cmo_monthly_above_25 | boolean | CMO monthly > 25 threshold |
| atr_14 | double | Average True Range |
| momentum_12_1_pct | double | 12-month minus 1-month return (academic momentum factor) |
| momentum_rank_in_market | bigint | Rank by momentum_12_1_pct within market |
| momentum_score | int | 0–100 bucket score from momentum_rank |
| cmo_score | int | 0–100 from CMO signal |
| rsi_score | int | 0–100 from RSI level |
| macd_score | int | 0–100 from MACD histogram direction |
| trend_score | int | 0–100 from SMA50/200 position |
| total_score | numeric | Weighted composite of 5 sub-scores |
| overall_rank | bigint | Rank by total_score across full universe |
| risk_rank | bigint | Rank by volatility |
| risk_bucket | text | `low` / `medium` / `high` |
| atr_pct | double | ATR as % of close (volatility measure) |
| volatility_pct_in_sector | double | Relative volatility vs sector peers |
| is_rankable | boolean | Has sufficient history and valid data |
| is_backtest_tradable | boolean | Passes all quality filters for backtesting |
| quality_reasons | text | Comma-separated reasons if not tradable |

**Size:** 210 MB · 771,947 rows · daily since 2021-05-28

---

### Intelligence OBTs (Primary Product Tables)

These are the **main tables for dashboards and the app**. Each is a wide OBT with zero join requirements.

#### `mart_stock_intel_weekly` / `_monthly` / `_quarterly` / `_6m`

One row per (ticker, market, period_start). All 4 share identical columns.

| Column | Type | Description |
|--------|------|-------------|
| ticker / market / asset_type | text | Identity |
| cadence | text | `weekly` / `monthly` / `quarterly` / `6m` |
| period_start | timestamp | First day of the period |
| period_end | timestamp | Last trading day of the period (score date) |
| score_date | timestamp | Date signals were computed |
| name / sector / industry | text | Company metadata |
| close | double | Close price at period_end |
| **period_return_pct** | double | Price return during this period (close / prev_close - 1) |
| total_score | numeric | Composite score 0–100 |
| momentum_score / cmo_score / rsi_score / macd_score / trend_score | int | Sub-scores 0–100 |
| **score_delta** | double | Score change vs previous period |
| **rank_delta** | bigint | Rank change vs previous period (negative = improved) |
| overall_rank | bigint | Rank in full universe (1 = best) |
| momentum_rank_in_market | bigint | Rank by raw momentum factor |
| risk_bucket | text | `low` / `medium` / `high` |
| risk_rank | bigint | Volatility rank |
| atr_pct | double | Volatility as % of price |
| volatility_pct_in_sector | double | Relative volatility vs sector peers |
| momentum_12_1_pct | double | Academic momentum factor % |
| rsi_14 / macd_hist / cmo_10 / sma_20 / sma_50 / sma_200 | double | Raw indicators |
| industry_momentum_avg | double | Average momentum of industry peers |
| industry_momentum_pct_rank | double | Stock's momentum percentile in its industry |
| **sector_avg_score** | double | Average total_score of sector peers this period |
| **sector_rank_in_market** | int | Sector rank (1 = top sector in market) |
| **macro_regime** | text | 5-state: `risk_on` / `constructive` / `neutral` / `defensive` / `risk_off` |
| market_regime | text | Legacy 3-state: `risk_on` / `neutral` / `risk_off` |
| risk_on_score | double | Composite macro score 0–100 (alias) |
| **composite_macro_score** | double | Macro score 0–100 |
| **credit_score** | double | Credit dimension 0–100 |
| **growth_score** | double | Growth dimension 0–100 |
| **dollar_score** | double | Dollar dimension 0–100 |
| benchmark_return_20d_pct | double | S&P 500 20-day return on score_date |
| **action_signal** | text | `strong` / `rising` / `fading` / `weak` / `neutral` |
| is_rankable / is_valid_market_data / is_backtest_tradable | boolean | Quality flags |
| quality_reasons | text | Why not tradable (if applicable) |

**action_signal logic:**
- `strong` — score ≥ 70 AND rank not falling (rank_delta ≤ 10)
- `rising` — score ≥ 50 AND rank improving fast (rank_delta ≤ −20)
- `fading` — score ≥ 50 AND rank falling fast (rank_delta ≥ 20)
- `weak` — score < 40
- `neutral` — everything else

**Coverage:**

| Mart | Rows | Tickers | Periods | Date Range |
|------|------|---------|---------|------------|
| `mart_stock_intel_weekly` | 126,581 | 618 | 207 weeks | 2022-06-13 → 2026-05-25 |
| `mart_stock_intel_monthly` | 29,345 | 618 | 48 months | 2022-06-01 → 2026-05-01 |
| `mart_stock_intel_quarterly` | 10,283 | 618 | 17 quarters | 2022-04-01 → 2026-04-01 |
| `mart_stock_intel_6m` | 5,363 | 618 | 9 half-years | 2022-01-01 → 2026-01-01 |

**Latest weekly signal distribution (2026-05-25):**
- 25 `strong` · 140 `rising` · 253 `neutral` · 66 `fading` · 134 `weak`

---

### Market Context

#### `mart_sector_scores_daily`
One row per (market, sector, score_date). Sector-level aggregates.

| Column | Description |
|--------|-------------|
| market / sector | Identity |
| avg_total_score / median_total_score | Score distribution |
| avg_momentum_12_1_pct | Average momentum factor |
| breadth_above_sma_200_pct | % of stocks above 200-day SMA |
| high_risk_pct | % of stocks in high risk bucket |
| sector_rank_overall / sector_rank_in_market | Rank (1 = best) |
| top_ticker / top_name / top_total_score | Sector leader |

#### `mart_sector_ranker_weekly`
Weekly snapshot of sector rankings with week-over-week context. Adds `weekly_sector_rank_in_market`.

#### `mart_industry_scores_daily` / `mart_industry_ranker_weekly`
Same structure as sector models but at industry grain (25 industry columns).

#### `mart_universe_health_current`
Current state of all 619 tickers: `ok` / `not_rankable_latest` / `missing_sector_or_industry` / `no_price_history`.

---

### Macro Regime

#### `mart_macro_regime_daily`
One row per trading day. The composite macro environment signal.

**6 dimensions, each scored 0–100:**

| Dimension | Weight | Key signals |
|-----------|--------|-------------|
| credit | 25% | HYG + LQD 20d return and SMA50 position |
| growth | 25% | DBB, CPER, HG=F, EEM, EFA — metals + EM breadth |
| dollar | 15% | UUP SMA20/50 + 20d return (strong dollar = lower score) |
| sector | 15% | Cyclicals (XLK/XLF/XLI/XLE) vs defensives (XLU/XLV) |
| rates | 10% | ^TNX direction + TLT rally context (disambiguates easing vs flight-to-safety) |
| inflation | 10% | GLD + CL=F spike = lower score; disinflation = higher |

**Composite formula:** `credit×0.25 + growth×0.25 + dollar×0.15 + sector×0.15 + rates×0.10 + inflation×0.10`

**5-state regime labels:**

| Score | macro_regime | Legacy market_regime |
|-------|-------------|----------------------|
| ≥ 80 | risk_on | risk_on |
| ≥ 60 | constructive | risk_on |
| ≥ 40 | neutral | neutral |
| ≥ 20 | defensive | risk_off |
| < 20 | risk_off | risk_off |

**Historical distribution (2021–2026):**
- constructive: 403 days (32%) · neutral: 342 (27%) · defensive: 296 (23%) · risk_on: 119 (9%) · risk_off: 98 (8%)

**Current (2026-05-28):** `constructive` · score 74 · credit 100 · growth 100 · inflation 70 · sector 60 · rates 50 · dollar 20

**Hysteresis thresholds (for strategy rules, not labels):** enter risk_on ≥70, exit <55; enter risk_off ≤30, exit >45.

Additional columns: `composite_macro_score_30d_avg`, `composite_macro_score_trend`, benchmark S&P columns, all 6 dimension scores.

#### `mart_market_regime_daily`
Passthrough `SELECT * FROM int_macro_regime_daily`. Kept for downstream compatibility.

---

### Product Tables

#### `mart_portfolio_ranker_weekly`
Weekly top-ranked stocks. 19 columns. Lighter version of `mart_stock_intel_weekly` (no delta, no macro, no sector context). Use `mart_stock_intel_weekly` instead for new work.

#### `mart_product_top_lists_current`
Current top lists with action labels. 15 columns.

| Column | Description |
|--------|-------------|
| list_name | e.g. `top_10` |
| list_rank | Position in list |
| ticker / market / name / sector / industry | Identity |
| close / total_score | Price and score |
| risk_bucket | `low` / `medium` / `high` |
| action_bucket | Categorical action |
| primary_signal_reason | Human-readable reason string |
| risk_note / product_disclaimer | Compliance-friendly labels |

#### `mart_product_stock_recommendations_daily` / `_weekly`
Full recommendations with signal reasons for all rankable stocks.

---

### Backtest Engine

48 momentum strategy variants backtested from 2022-06-20 to 2026-05-28.

**Strategy dimensions:**
- Family: `momentum_base` / `momentum_academic` / `momentum_risk_adjusted`
- Rebalance: `weekly` / `monthly` / `quarterly` / `6m`
- Portfolio size: top 10 / 20 / 30 / 40

**Key tables:**

| Table | Rows | Description |
|-------|------|-------------|
| `mart_backtest_trades` | 128,714 | Every trade: entry/exit date, prices, costs, return |
| `mart_backtest_performance` | 48 | Per-strategy: Sharpe, annualized return, max drawdown, costs |
| `mart_backtest_signals_daily` | 1.8M | Daily signal scores for all strategies |
| `mart_backtest_portfolio_equity_daily` | ~7MB | Portfolio equity curve per strategy |
| `mart_backtest_production_candidates` | 30 | Strategies passing production quality thresholds |

**Top 5 strategies by Sharpe:**

| Strategy | Sharpe | Ann. Return | Max Drawdown |
|----------|--------|-------------|--------------|
| momentum_academic_monthly_top10 | 2.36 | 106% | -30.5% |
| momentum_academic_quarterly_top10 | 2.07 | 91% | -37.4% |
| momentum_risk_adjusted_monthly_top20 | 2.04 | 54% | -18.6% |
| momentum_base_monthly_top10 | 2.02 | 55% | -17.6% |
| momentum_base_monthly_top20 | 1.94 | 46% | -17.2% |

Note: entry = open of day after rebalance close. No look-ahead bias.
Note: high annualized returns reflect 2022–2026 period (bear bottom → AI bull run). Signal validity horizon is 60-day+, not 20-day.

**`mart_backtest_trades` key columns:**

| Column | Description |
|--------|-------------|
| backtest_id | Strategy identifier |
| rebalance_period_start / rebalance_date | Period boundaries |
| entry_date / exit_date | Trade execution dates |
| entry_price / exit_price | Open price on execution day |
| gross_return_pct / net_return_pct | Pre/post cost returns |
| total_cost_pct / entry_cost_pct / exit_cost_pct / slippage_pct | Cost breakdown |
| signal_rank / total_score | Why this stock was selected |
| risk_bucket | Risk classification at entry |

---

### Briefing Marts (Internal / Legacy)

`mart_briefing_portfolio_daily` / `_weekly` / `_monthly` / `_quarterly` / `_6m`

38–40 column wide tables used as the base layer for `mart_stock_intel_*`. Still valid but the intelligence OBTs are the preferred query target.

---

## Data Quality (as of 2026-05-28)

### Universe Health
| Status | Tickers |
|--------|---------|
| ok | 602 |
| not_rankable_latest | 15 |
| missing_sector_or_industry | 1 |
| no_price_history | 1 |

### Market Quality (equity universe)
| Flag | Count | % of rows |
|------|-------|-----------|
| tradable_rows | 745,287 | 96.5% |
| low_liquidity | 22,829 | 3.0% |
| invalid_ohlc | 90 | 0.01% |
| stale_price | 69 | 0.01% |
| extreme_return | 6 | <0.01% |

### Macro Data Quality
- 26/26 tickers, 0 null closes, 0 zero closes
- Minor row count variance (1255–1258) due to futures trading extra days vs ETFs

---

## Ingest Schedule (Dagster)

| Job | Schedule | What it does |
|-----|----------|-------------|
| `equity_job` | 22:00 UTC weekdays | Equity OHLCV + ticker metadata |
| `macro_job` | 22:15 UTC weekdays | 26 macro instruments OHLCV |
| `index_job` | 22:30 UTC weekdays | 6 benchmark indexes OHLCV |
| `snapshot_job` | Weekly | Ticker metadata snapshot |
| `portfolio_ranker_job` | Weekly | Top-ranked stock lists |
| `backtest_job` | Weekly | Re-run all 48 backtest strategies |

All jobs run full dbt pipeline after ingest (staging → intermediate → marts).
Backfill: 5 years on first run, watermark-based incremental thereafter.

---

## Total Storage

~2.5 GB across 56 tables. Dominated by backtest signal/recommendation tables (567 MB + 276 MB). Core price + scoring data is ~600 MB.

---

## What Is NOT Here

- User portfolios (planned — one table: user_id, ticker, market, shares, cost_basis)
- Portfolio-level intelligence mart (blocked on user data)
- Intraday data (daily only)
- Fundamentals / earnings / financials
- News / sentiment
- Crypto (removed)
- Options data
