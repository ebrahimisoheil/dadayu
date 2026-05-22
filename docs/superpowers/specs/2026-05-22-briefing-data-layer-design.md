# Briefing Data Layer — Design Spec

**Date:** 2026-05-22
**Status:** Approved

---

## Problem

The warehouse has good scoring marts (`mart_portfolio_asset_scores_daily`, `mart_themes_scores_daily`, `mart_market_regime_daily`) but they are siloed. No single model unifies:

- Asset momentum scores + risk classification
- Market regime context (is it a risk-on or risk-off day?)
- Theme membership + theme health signals
- Polymarket confirmation flowing back to individual assets
- Industry peer comparison

The briefing layer solves this: one pre-joined mart per rebalancing frequency, structured to power user-facing briefings with no joins at query time. Academic momentum backtest variants validate whether pure 12-1 momentum outperforms the current composite scoring.

---

## Architecture

```
02_intermediate/
  market/
    int_market_asset_indicators_daily    ← ADD cmo_monthly_10, cmo_monthly_above_25

03_marts/
  portfolio/
    mart_portfolio_asset_scores_daily    ← ADD risk_bucket, atr_pct, volatility_pct_in_sector
  themes/
    mart_themes_scores_daily             ← unchanged
    mart_themes_alerts_daily             ← unchanged
    + mart_asset_theme_intel_daily       ← NEW: theme + PM intel per asset per day
  market/
    mart_market_regime_daily             ← unchanged
  backtests/
    mart_backtest_signals_daily          ← ADD academic momentum variants
  briefing/                              ← NEW folder
    mart_briefing_portfolio_daily        ← NEW: base, all joins, all fields
    mart_briefing_portfolio_weekly       ← NEW: thin snapshot of daily
    mart_briefing_portfolio_monthly      ← NEW: thin snapshot of daily
    mart_briefing_portfolio_quarterly    ← NEW: thin snapshot of daily
    mart_briefing_portfolio_6m           ← NEW: thin snapshot of daily
```

**Layering rule:** The briefing layer reads only from marts. No intermediate is referenced directly from `03_marts/briefing/`.

---

## Part 1 — Indicator Additions

### `int_market_asset_indicators_daily`

Add two columns to match the academic CMO signal from the friend's screener:

**`cmo_monthly_10`** — CMO(10) computed over 21-bar rolling window (≈ 1 calendar month):
```sql
((cmo_monthly_gains - cmo_monthly_losses)
    / nullIf(cmo_monthly_gains + cmo_monthly_losses, 0)) * 100
```
Where:
- `cmo_monthly_gains` = `sumIf(gain, gain > 0) OVER (... ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)`
- `cmo_monthly_losses` = `sumIf(loss, loss > 0) OVER (... ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)`
- `gain` and `loss` are already computed in the model's `prepped` CTE

**`cmo_monthly_above_25`** — `cmo_monthly_10 > 25` (Bool). The entry gate used in the academic strategy.

---

## Part 2 — Risk Classification

### `mart_portfolio_asset_scores_daily`

Also pass through `cmo_monthly_10` and `cmo_monthly_above_25` from `int_market_asset_indicators_daily` in the SELECT clause so they are available in the briefing layer.

Add three new columns:

**`atr_pct`** — normalised volatility:
```sql
atr_14 / nullIf(close, 0) AS atr_pct
```

**`volatility_pct_in_sector`** — percentile rank within (score_date, sector):
```sql
percent_rank() OVER (
    PARTITION BY score_date, sector
    ORDER BY atr_pct ASC
) AS volatility_pct_in_sector
```
Higher percentile = higher volatility relative to sector peers.

Edge case: sectors with fewer than 5 rankable assets on a given date fall back to partitioning by `asset_type` (equity/crypto) instead of sector, to avoid meaningless within-sector percentiles. Crypto always uses asset_type partition since all crypto assets sit in the same synthetic 'Crypto' sector.

**`risk_bucket`**:
```sql
multiIf(
    volatility_pct_in_sector >= 0.67, 'high',
    volatility_pct_in_sector >= 0.33, 'medium',
    'low'
) AS risk_bucket
```

`risk_bucket` is a scoring concern — it belongs in the mart, not the briefing layer. It is used for `risk_rank` partitioning in the briefing layer and for backtest analysis (does academic momentum perform differently across risk clusters?).

---

## Part 3 — Asset Theme Intel Mart

### `mart_asset_theme_intel_daily` (new)

**Location:** `03_marts/themes/`
**Materialization:** `table`, `MergeTree()`, ordered by `(ticker, market, score_date)`
**Sources:** `int_themes_constituents`, `mart_themes_scores_daily`, `mart_themes_alerts_daily`, `int_themes_pm_signals_daily`

Reverses the theme → constituent direction. One row per (ticker, market, score_date).

**Logic:**
1. Join `int_themes_constituents` → `mart_themes_scores_daily` to get scores for every theme each asset belongs to
2. Pick `top_theme` = theme with highest `total_theme_score` for this asset on this date
3. Aggregate PM signals across all themes this asset belongs to, weighted by `normalized_theme_weight`
4. Join `mart_themes_alerts_daily` to get highest alert severity across the asset's themes

**Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `ticker` | String | Asset ticker |
| `market` | String | Market |
| `score_date` | DateTime | Date |
| `theme_count` | Int | Number of themes this asset belongs to |
| `theme_ids` | Array(String) | All theme IDs for this asset |
| `top_theme_id` | String | Theme with highest `total_theme_score` today |
| `top_theme_name` | String | Display name of top theme |
| `top_theme_score` | Float64 | `total_theme_score` of top theme (0–100) |
| `top_theme_rank` | Int | Global rank of top theme among all themes today |
| `top_theme_alert_severity` | String | `high`, `medium`, `low`, or null — from `mart_themes_alerts_daily` |
| `pm_score` | Float64 | Weighted avg PM score across all themes this asset belongs to. Null if no PM signals. |
| `pm_signal_available` | Bool | True if any of the asset's themes has `pm_signal_available = true` |
| `pm_probability_change` | Float64 | Weighted avg `avg_probability_change` across themes. Positive = PM momentum building. |
| `pm_market_count` | Int | Total PM markets active across this asset's themes |
| `pm_confirmation` | Bool | `pm_signal_available AND pm_score > 65 AND` (asset's `total_score > 70` — joined at briefing layer, set false here as placeholder) |

**Note:** `pm_confirmation` at this mart level is set to `pm_signal_available AND pm_score > 65`. The full confirmation (which also requires asset momentum score > 70) is computed in `mart_briefing_portfolio_daily` where both values are available.

---

## Part 4 — Briefing Layer

### `mart_briefing_portfolio_daily` (new, base mart)

**Location:** `03_marts/briefing/`
**Materialization:** `table`, `MergeTree()`, ordered by `(score_date, risk_bucket, overall_rank)`, partitioned by month
**Pre-hook:** `SET max_partitions_per_insert_block = 0`
**Sources:** `mart_portfolio_asset_scores_daily`, `mart_asset_theme_intel_daily`, `mart_market_regime_daily`

One row per (ticker, market, score_date). All joins happen here.

**Columns:**

*Identity:*
| Column | Description |
|--------|-------------|
| `ticker`, `market`, `asset_type` | Asset identifier |
| `score_date` | Date |
| `name`, `sector`, `industry` | Dimensional |

*Prices + momentum:*
| Column | Description |
|--------|-------------|
| `close` | Close price |
| `momentum_12_1_pct` | Core 12-1 academic momentum signal |
| `momentum_rank_in_market` | Rank within market by momentum |
| `cmo_10` | Daily CMO(10) — from indicators via scores mart |
| `cmo_monthly_10` | Monthly CMO(10) |
| `cmo_monthly_above_25` | Entry gate flag |
| `rsi_14`, `macd_hist`, `sma_20`, `sma_50`, `sma_200` | Supporting indicators |

*Scoring:*
| Column | Description |
|--------|-------------|
| `total_score` | Composite score (0–100) |
| `momentum_score`, `cmo_score`, `rsi_score`, `macd_score`, `trend_score` | Score components |
| `is_rankable` | Has sufficient history for a valid score |
| `risk_bucket` | `high`, `medium`, `low` |
| `atr_pct` | Normalised volatility |

*Rankings (computed in this mart):*
| Column | Description |
|--------|-------------|
| `overall_rank` | Rank across all rankable assets on this date, ordered by `total_score DESC` |
| `risk_rank` | Rank within (score_date, risk_bucket), ordered by `total_score DESC` |
| `industry_momentum_avg` | Avg `momentum_12_1_pct` of all rankable assets in same GICS industry. Measures whether asset outperforms peers. |
| `industry_momentum_pct_rank` | Percentile rank within industry by momentum |

*Market regime (stamped on every row for the date):*
| Column | Description |
|--------|-------------|
| `market_regime` | `risk_on`, `neutral`, `risk_off` — derived from SP500 SMA50/SMA200. Stamped on all assets including German/Indian equities — US market regime is treated as global macro context. |
| `risk_on_score` | 80/50/20 score from regime mart |
| `benchmark_return_20d_pct` | SP500 20-day return — macro context for briefing |

*Theme intel:*
| Column | Description |
|--------|-------------|
| `theme_count` | Number of themes this asset belongs to |
| `theme_ids` | Array of theme IDs |
| `top_theme_id`, `top_theme_name` | Highest-scoring theme today |
| `top_theme_score` | Score of top theme (0–100) |
| `top_theme_rank` | Rank of top theme globally |
| `top_theme_alert_severity` | Alert level: `high`, `medium`, `low`, or null |

*PM intel:*
| Column | Description |
|--------|-------------|
| `pm_score` | Weighted PM score across asset's themes |
| `pm_signal_available` | Any PM signal active for this asset's themes |
| `pm_probability_change` | Directional PM momentum (positive = building conviction) |
| `pm_market_count` | Active PM markets touching this asset's themes |
| `pm_confirmation` | `pm_signal_available AND pm_score > 65 AND total_score > 70` — high-conviction cross-signal flag |

---

### Frequency Snapshot Marts (4 thin mats)

**Pattern:** Each frequency mart selects from `mart_briefing_portfolio_daily` for the last available `score_date` within each period. No additional joins. Identical schema to the daily mart plus two period columns.

**Shared logic (example for monthly):**
```sql
WITH last_date_per_period AS (
    SELECT
        toStartOfMonth(score_date) AS period_start,
        max(score_date) AS snapshot_date
    FROM mart_briefing_portfolio_daily
    WHERE is_rankable
    GROUP BY period_start
)
SELECT
    b.*,
    p.period_start,
    p.snapshot_date AS period_end
FROM mart_briefing_portfolio_daily b
INNER JOIN last_date_per_period p ON b.score_date = p.snapshot_date
```

| Mart | Period function | Period label |
|------|----------------|--------------|
| `mart_briefing_portfolio_weekly` | `toStartOfWeek(score_date)` | `week_start` |
| `mart_briefing_portfolio_monthly` | `toStartOfMonth(score_date)` | `month_start` |
| `mart_briefing_portfolio_quarterly` | `toStartOfQuarter(score_date)` | `quarter_start` |
| `mart_briefing_portfolio_6m` | `if(toMonth(score_date) <= 6, toStartOfYear(score_date), addMonths(toStartOfYear(score_date), 6))` | `half_year_start` |

**Query pattern for product layer:**
```sql
-- Top 10 high-risk assets this week
SELECT ticker, market, name, momentum_12_1_pct, top_theme_name, pm_confirmation
FROM mart_briefing_portfolio_weekly
WHERE risk_bucket = 'high'
  AND is_rankable
  AND week_start = (SELECT max(week_start) FROM mart_briefing_portfolio_weekly)
ORDER BY risk_rank ASC
LIMIT 10
```

---

## Part 5 — Academic Momentum Backtest Variants

### `mart_backtest_signals_daily` additions

Five new variants using pure `momentum_12_1_pct` ranking — no composite score. This validates the academic thesis (Jegadeesh & Titman) against the current composite scoring.

**Pure momentum candidates CTE** (new, alongside existing `base_candidates` and `pm_candidates`):
```sql
academic_candidates AS (
    SELECT
        *,
        false AS uses_pm_signal,
        momentum_12_1_pct AS strategy_score
    FROM asset_scores
    WHERE is_rankable
),

academic_cmo_candidates AS (
    SELECT * FROM academic_candidates
    WHERE cmo_monthly_above_25 = true  -- entry gate
)
```

**New backtest IDs:**

| backtest_id | Ranking | Entry gate | Rebalance |
|-------------|---------|------------|-----------|
| `momentum_academic_weekly_top40` | momentum_12_1_pct | none | weekly |
| `momentum_academic_monthly_top40` | momentum_12_1_pct | none | monthly |
| `momentum_academic_quarterly_top40` | momentum_12_1_pct | none | quarterly |
| `momentum_academic_6m_top40` | momentum_12_1_pct | none | 6-monthly |
| `momentum_academic_cmo_monthly_top40` | momentum_12_1_pct | cmo_monthly_above_25 | monthly |

`mart_backtest_trades` already handles any `backtest_id` generically — no changes needed there.

---

## What Is Not In Scope

- BTC/ETH ↔ equity rolling correlation (complex window self-join, deferred)
- Direct PM → asset links via `linked_asset` field (currently only theme-mediated path)
- Portfolio ingestion (user-provided ticker lists)
- Pricing tier gating
- Briefing text generation (Python layer, separate concern)
- Any changes to ingestion assets or Dagster schedules (the briefing marts are driven by existing daily `portfolio_ranker_schedule`)

---

## Model Change Summary

| Model | Type | Change |
|-------|------|--------|
| `int_market_asset_indicators_daily` | modify | add `cmo_monthly_10`, `cmo_monthly_above_25` |
| `mart_portfolio_asset_scores_daily` | modify | add `atr_pct`, `volatility_pct_in_sector`, `risk_bucket` |
| `mart_asset_theme_intel_daily` | new | theme + PM intel per asset per day |
| `mart_briefing_portfolio_daily` | new | base briefing mart, all joins |
| `mart_briefing_portfolio_weekly` | new | thin weekly snapshot |
| `mart_briefing_portfolio_monthly` | new | thin monthly snapshot |
| `mart_briefing_portfolio_quarterly` | new | thin quarterly snapshot |
| `mart_briefing_portfolio_6m` | new | thin 6-month snapshot |
| `mart_backtest_signals_daily` | modify | add 5 academic momentum variants |

**Total: 2 modified, 7 new.**
