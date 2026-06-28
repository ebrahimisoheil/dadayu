# DADAYU Cube API — Developer Reference

## Connection

| | |
|---|---|
| Base URL | `http://<server>:4000` |
| Auth header | `Authorization: Bearer dadayu_cube_secret_change_in_prod` |
| Query endpoint | `POST /cubejs-api/v1/load` |
| Content-Type | `application/json` |

All queries use the Cube JSON query format:

```json
{
  "query": {
    "dimensions": ["CubeName.fieldName"],
    "measures": ["CubeName.measureName"],
    "filters": [...],
    "segments": ["CubeName.segmentName"],
    "order": [["CubeName.fieldName", "asc"]],
    "limit": 50
  }
}
```

---

## Cubes

### 1. `StockIntelligence`

**Source:** UNION ALL of `mart_stock_intel_weekly`, `_monthly`, `_quarterly`, `_6m`  
**Grain:** one row per `(ticker, market, cadence, period_start)`  
**Use for:** stock cards, score tables, rebalance picks, historical signal timeline

#### Key Dimensions

| Field | Type | Values / Notes |
|---|---|---|
| `ticker` | string | e.g. `HOT.DE`, `AAPL` |
| `market` | string | `us` or `germany` |
| `name` | string | company name |
| `sector` / `industry` | string | GICS classification |
| `cadence` | string | `weekly` `monthly` `quarterly` `6m` |
| `period_start` | time | start of scoring window |
| `period_end` | time | end of scoring window |
| `score_date` | time | date scores were computed |
| `action_signal` | string | `strong` `rising` `fading` `weak` `neutral` |
| `risk_bucket` | string | `low` `medium` `high` |
| `macro_regime` | string | `risk_on` `constructive` `neutral` `defensive` `risk_off` |
| `total_score` | number | 0–100 composite momentum score |
| `momentum_rank_in_market` | number | rank within market (lower = better) |
| `overall_rank` | number | cross-market rank |
| `momentum_12_1_pct` | number | 12-month return skipping last month (%) |
| `period_return_pct` | number | return during the scoring period (%) |
| `close` / `sma_50` / `sma_200` | number | price levels |
| `rsi_14` / `macd_hist` / `cmo_10` | number | raw technical indicators |
| `score_delta` | number | score change vs previous period |
| `rank_delta` | number | rank change (negative = improved) |
| `is_rankable` | boolean | passes all 4 momentum filters |
| `composite_macro_score` | number | macro score snapshot at scoring time |

#### Computed Dimensions (no DB column, use directly)

| Field | Values |
|---|---|
| `score_tier` | `A — Strong` `B — Good` `C — Neutral` `D — Weak` `E — Poor` |
| `rank_movement` | `Strong Climb` `Climbing` `Stable` `Slipping` `Falling` |
| `momentum_label` | `Explosive (>100%)` `Strong (50-100%)` `Moderate (20-50%)` `Mild (0-20%)` `Negative` |
| `return_direction` | `Up >5%` `Up 0-5%` `Flat` `Down 0-5%` `Down >5%` |
| `sector_rank_label` | `Top Sector` `Top 3 Sector` `Top 5 Sector` `Other` |

#### Measures

| Measure | Description |
|---|---|
| `count` | stock count |
| `avg_total_score` | avg score across selection |
| `rankable_count` | stocks passing all 4 filters |
| `strong_count` / `rising_count` / `fading_count` / `weak_count` | signal breakdown |
| `high_risk_count` / `low_risk_count` | risk breakdown |
| `above_sma200_count` | breadth |
| `avg_momentum_12_1` | avg 12-1 momentum |
| `avg_period_return_pct` | avg period return |

#### Segments (pre-built filters)

| Segment | What it does |
|---|---|
| `StockIntelligence.rankable_only` | `is_rankable = true` |
| `StockIntelligence.us_market` | `market = 'us'` |
| `StockIntelligence.germany_market` | `market = 'germany'` |
| `StockIntelligence.weekly_cadence` | `cadence = 'weekly'` |
| `StockIntelligence.monthly_cadence` | `cadence = 'monthly'` |
| `StockIntelligence.quarterly_cadence` | `cadence = 'quarterly'` |
| `StockIntelligence.cadence_6m` | `cadence = '6m'` |
| `StockIntelligence.strong_signal` | `action_signal = 'strong'` |
| `StockIntelligence.actionable_signal` | `action_signal IN ('strong','rising')` |
| `StockIntelligence.high_risk` | `risk_bucket = 'high'` |
| `StockIntelligence.low_risk` | `risk_bucket = 'low'` |
| `StockIntelligence.above_sma200` | `close > sma_200` |
| `StockIntelligence.top_quartile` | `total_score >= 60` |
| `StockIntelligence.risk_on_macro` | macro regime = risk_on or constructive |

---

### 2. `MacroRegime`

**Source:** `mart_macro_regime_daily`  
**Grain:** one row per date  
**Use for:** macro context panel, regime history chart, regime badge

#### Key Dimensions

| Field | Type | Notes |
|---|---|---|
| `ts` | time | date (primary key) |
| `macro_regime` | string | `risk_on` `constructive` `neutral` `defensive` `risk_off` |
| `market_regime` | string | `risk_on` `neutral` `risk_off` (3-state) |
| `regime_is_positive` | string | `Positive` `Neutral` `Negative` |
| `composite_macro_score` | number | 0–100 |
| `composite_macro_score_30d_avg` | number | 30-day rolling avg |
| `composite_macro_score_trend` | number | vs 30d avg (positive = above) |
| `credit_score` | number | HYG/LQD health (0–100) |
| `growth_score` | number | metals + EM breadth (0–100) |
| `dollar_score` | number | inverted: low = strong dollar = risk-off |
| `sector_score` | number | cyclicals vs defensives leadership |
| `rates_score` | number | bond market context |
| `inflation_score` | number | high = disinflation |
| `benchmark_close` | number | S&P 500 close |
| `benchmark_above_sma_200` | boolean | S&P 500 above 200-day SMA |

#### Segments

| Segment | What it does |
|---|---|
| `MacroRegime.risk_on` | `macro_regime IN ('risk_on','constructive')` |
| `MacroRegime.risk_off` | `macro_regime IN ('risk_off','defensive')` |
| `MacroRegime.neutral` | `macro_regime = 'neutral'` |
| `MacroRegime.trending_up` | `composite_macro_score_trend > 0` |

---

### 3. `SectorContext`

**Source:** `mart_sector_ranker_weekly`  
**Grain:** one row per `(market, sector, week_start)`  
**Use for:** sector heatmap, sector rotation chart, top sector badge

#### Key Dimensions

| Field | Notes |
|---|---|
| `market` | `us` or `germany` |
| `sector` | GICS sector name |
| `week_start` / `score_date` | timing |
| `sector_rank_in_market` | 1 = strongest sector in this market |
| `avg_total_score` | avg stock score across sector |
| `breadth_above_sma_200_pct` | % of stocks above 200-day SMA |
| `high_risk_pct` | % of stocks in high risk bucket |
| `rankable_count` | # stocks passing filters |
| `top_ticker` / `top_name` | strongest stock in sector |
| `rank_badge` | `Gold #1` `Silver #2` `Bronze #3` `Other` |
| `strength_label` | `Strong` `Moderate` `Weak` `Poor` |
| `breadth_label` | `Broad Uptrend` `Mixed` `Mostly Below` `Downtrend` |

---

### 4. `IndustryContext`

**Source:** `mart_industry_ranker_weekly`  
**Grain:** one row per `(market, sector, industry, week_start)`  
**Use for:** industry drill-down table

Key fields same as SectorContext plus `industry_rank_in_market` and `industry_rank_in_sector`.

---

### 5. `BacktestPerformance`

**Source:** `mart_backtest_production_candidates`  
**Grain:** one row per strategy variant  
**Use for:** strategy comparison table, performance scorecard

#### Key Dimensions

| Field | Notes |
|---|---|
| `backtest_id` | unique strategy ID |
| `strategy_family` | e.g. `momentum_academic_monthly_top10` |
| `rebalance_frequency` | `monthly` `weekly` `quarterly` |
| `portfolio_size` | `10` `20` |
| `universe_scope` | e.g. `us` `germany` `global` |
| `sharpe_ratio` | primary performance metric |
| `annualized_return_pct` | annualized return % |
| `max_drawdown_pct` | max drawdown % |
| `win_rate_pct` | % of profitable trades |
| `calmar_ratio` | return / max drawdown |
| `alpha_pct` | alpha vs benchmark |
| `annualized_cost_drag_bps` | transaction cost per year |
| `is_production_candidate` | boolean — meets min thresholds |
| `performance_tier` | `Exceptional` `Strong` `Good` `Below Target` |
| `risk_profile` | `Conservative` `Moderate` `Aggressive` |

Best strategy currently: `momentum_academic_monthly_top10` → **52.37% annualized, Sharpe 1.58**

#### Segments

| Segment | What it does |
|---|---|
| `BacktestPerformance.production_only` | `is_production_candidate = true` |
| `BacktestPerformance.monthly_rebalance` | monthly strategies |
| `BacktestPerformance.top10_portfolio` | portfolio_size = 10 |
| `BacktestPerformance.low_cost` | cost drag ≤ 300bps |

---

### 6. `BacktestTrades`

**Source:** `mart_backtest_trades`  
**Grain:** one row per trade (backtest_id + rebalance_date + ticker + market)  
**Use for:** trade log, win/loss analysis, ticker contribution

Key fields: `ticker`, `market`, `entry_date`, `exit_date`, `entry_price`, `exit_price`, `gross_return_pct`, `net_return_pct`, `holding_days`, `signal_rank`, `risk_bucket`, `trade_outcome`

---

### 7. `ProductTopLists`

**Source:** `mart_product_top_lists_current` (current period only, not historical)  
**Grain:** one row per `(list_name, ticker, market)`  
**Use for:** "what to buy this month" snapshot

> ⚠️ **Known limitation:** `list_rank` is a global rank (not per-market). `top_10` gives 5 DE + 5 US, not 10 per market. For per-market top-10, use `StockIntelligence` with `momentum_rank_in_market <= 10` (see recipes below).

| Field | Notes |
|---|---|
| `list_name` | `top_10` `top_20` `top_30` |
| `list_rank` | global rank 1–30 |
| `ticker` / `market` / `name` / `sector` / `industry` | identity |
| `total_score` | momentum score |
| `risk_bucket` | `low` `medium` `high` |
| `action_bucket` | action classification |
| `primary_signal_reason` | human-readable signal explanation |
| `risk_note` | risk warning text |
| `product_disclaimer` | disclaimer text |
| `close` | last close price |

---

### 8. `UniverseHealth`

**Source:** `mart_universe_health_current`  
**Grain:** one row per ticker (current snapshot)  
**Use for:** universe coverage dashboard, missing data alerts

| Field | Values |
|---|---|
| `universe_status` | `ok` `not_rankable_latest` `missing_sector_or_industry` `no_price_history` |

---

## Views (pre-built)

Views are aliases to cubes with a curated field subset. Use them instead of cubes when available — they expose exactly what the use case needs.

| View | Use for |
|---|---|
| `WeeklyStocks` | weekly subscription tier / weekly digest |
| `MonthlyStocks` | monthly report page |
| `QuarterlyStocks` | quarterly review page |
| `SemiAnnualStocks` | 6-month review page |
| `CurrentWeekSnapshot` | "what's happening right now" dashboard |
| `MarketOverview` | macro + market breadth panels |
| `SectorLeaderboard` | sector rotation / heatmap |
| `TopResearchIdeas` | pre-filtered to actionable + rankable stocks |
| `StrategyComparison` | strategy table / risk-return scatter |
| `ScoreHistory` | per-ticker score trend chart |

> **Note:** `CurrentWeekSnapshot` still needs a `period_start` filter to the latest date — the view does not auto-filter.

---

## Query Recipes

### Current top 10 DE picks

```json
{
  "query": {
    "dimensions": [
      "StockIntelligence.ticker",
      "StockIntelligence.name",
      "StockIntelligence.sector",
      "StockIntelligence.total_score",
      "StockIntelligence.momentum_rank_in_market",
      "StockIntelligence.action_signal",
      "StockIntelligence.risk_bucket",
      "StockIntelligence.period_start"
    ],
    "filters": [
      {"member": "StockIntelligence.market", "operator": "equals", "values": ["germany"]},
      {"member": "StockIntelligence.momentum_rank_in_market", "operator": "lte", "values": ["10"]}
    ],
    "order": [["StockIntelligence.period_start", "desc"], ["StockIntelligence.momentum_rank_in_market", "asc"]],
    "limit": 10
  }
}
```

> Same query with `"values": ["us"]` for US top 10.

---

### Current macro regime (latest day)

```json
{
  "query": {
    "dimensions": [
      "MacroRegime.ts",
      "MacroRegime.macro_regime",
      "MacroRegime.market_regime",
      "MacroRegime.composite_macro_score",
      "MacroRegime.composite_macro_score_trend",
      "MacroRegime.credit_score",
      "MacroRegime.growth_score",
      "MacroRegime.dollar_score",
      "MacroRegime.sector_score",
      "MacroRegime.rates_score",
      "MacroRegime.inflation_score"
    ],
    "order": [["MacroRegime.ts", "desc"]],
    "limit": 1
  }
}
```

---

### Monthly rebalance diff (OUT/IN between two periods)

Fetch top 10 per market for **period A** and **period B** separately, then diff in the app layer:

```json
{
  "query": {
    "dimensions": ["StockIntelligence.ticker", "StockIntelligence.market", "StockIntelligence.period_start", "StockIntelligence.momentum_rank_in_market", "StockIntelligence.total_score"],
    "filters": [
      {"member": "StockIntelligence.period_start", "operator": "inDateRange", "values": ["2026-05-01", "2026-05-31"]},
      {"member": "StockIntelligence.momentum_rank_in_market", "operator": "lte", "values": ["10"]}
    ],
    "order": [["StockIntelligence.market", "asc"], ["StockIntelligence.momentum_rank_in_market", "asc"]]
  }
}
```

Run twice (change date range). App diff:
- `OUT` = in period A not in period B
- `IN` = in period B not in period A
- `HOLD` = in both

---

### Top sectors right now

```json
{
  "query": {
    "dimensions": [
      "SectorContext.market",
      "SectorContext.sector",
      "SectorContext.sector_rank_in_market",
      "SectorContext.rank_badge",
      "SectorContext.avg_total_score",
      "SectorContext.breadth_above_sma_200_pct",
      "SectorContext.top_ticker",
      "SectorContext.top_name"
    ],
    "order": [["SectorContext.score_date", "desc"], ["SectorContext.market", "asc"], ["SectorContext.sector_rank_in_market", "asc"]],
    "limit": 20
  }
}
```

---

### Best backtest strategies

```json
{
  "query": {
    "dimensions": [
      "BacktestPerformance.strategy_family",
      "BacktestPerformance.rebalance_frequency",
      "BacktestPerformance.portfolio_size",
      "BacktestPerformance.sharpe_ratio",
      "BacktestPerformance.annualized_return_pct",
      "BacktestPerformance.max_drawdown_pct",
      "BacktestPerformance.performance_tier"
    ],
    "segments": ["BacktestPerformance.production_only"],
    "order": [["BacktestPerformance.sharpe_ratio", "desc"]],
    "limit": 10
  }
}
```

---

### Score history for a single ticker

```json
{
  "query": {
    "dimensions": [
      "StockIntelligence.period_start",
      "StockIntelligence.cadence",
      "StockIntelligence.total_score",
      "StockIntelligence.action_signal",
      "StockIntelligence.momentum_rank_in_market",
      "StockIntelligence.score_delta"
    ],
    "filters": [
      {"member": "StockIntelligence.ticker", "operator": "equals", "values": ["HOT.DE"]},
      {"member": "StockIntelligence.cadence", "operator": "equals", "values": ["monthly"]}
    ],
    "order": [["StockIntelligence.period_start", "asc"]]
  }
}
```

---

### Rankable stock count by market (latest period only)

Add a `period_start` filter to the latest period date — otherwise the count spans all history:

```json
{
  "query": {
    "dimensions": ["StockIntelligence.market"],
    "measures": ["StockIntelligence.rankable_count"],
    "filters": [
      {"member": "StockIntelligence.cadence", "operator": "equals", "values": ["weekly"]},
      {"member": "StockIntelligence.period_start", "operator": "equals", "values": ["2026-06-22"]}
    ]
  }
}
```

---

## Data Freshness

| Table | Updated |
|---|---|
| OHLCV prices | daily (Dagster pipeline) |
| StockIntelligence | weekly (end of trading week) |
| MacroRegime | daily |
| SectorContext / IndustryContext | weekly |
| BacktestPerformance / BacktestTrades | monthly (or on-demand) |
| ProductTopLists | monthly (first trading day) |

Pipeline runs via Dagster (`http://<server>:3000`). Full run takes ~10–15 min on fresh DB.

---

## What Cube Cannot Do

These require app-layer logic or additional data sources not in the pipeline:

| Use case | Why |
|---|---|
| Live P&L per subscriber position | Needs actual entry prices from subscriber_holdings table |
| Live portfolio equity curve | Needs actual execution prices |
| Per-market top-10 from ProductTopLists | Global ranking bug — use StockIntelligence instead |
| Push alerts / webhooks | Cube is read-only query API |
