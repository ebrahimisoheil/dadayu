{{ config(materialized='table') }}

WITH base AS (
    SELECT
        *,
        week_start AS period_start
    FROM {{ ref('mart_briefing_portfolio_weekly') }}
),

with_deltas AS (
    SELECT
        *,
        close / NULLIF(
            LAG(close) OVER (PARTITION BY ticker, market ORDER BY period_start), 0
        ) - 1                                                                           AS period_return_pct,
        total_score - LAG(total_score) OVER (PARTITION BY ticker, market ORDER BY period_start) AS score_delta,
        overall_rank - LAG(overall_rank) OVER (PARTITION BY ticker, market ORDER BY period_start) AS rank_delta
    FROM base
),

sector_ctx AS (
    SELECT score_date, market, sector, avg_total_score AS sector_avg_score, sector_rank_in_market
    FROM {{ ref('mart_sector_scores_daily') }}
),

macro_ctx AS (
    SELECT ts, macro_regime, composite_macro_score, credit_score, growth_score, dollar_score
    FROM {{ ref('mart_macro_regime_daily') }}
)

SELECT
    d.ticker,
    d.market,
    d.asset_type,
    'weekly'            AS cadence,
    d.period_start,
    d.period_end,
    d.score_date,
    d.name,
    d.sector,
    d.industry,
    d.close,
    d.period_return_pct,
    d.total_score,
    d.momentum_score,
    d.cmo_score,
    d.rsi_score,
    d.macd_score,
    d.trend_score,
    d.score_delta,
    d.rank_delta,
    d.overall_rank,
    d.momentum_rank_in_market,
    d.risk_bucket,
    d.risk_rank,
    d.atr_pct,
    d.volatility_pct_in_sector,
    d.momentum_12_1_pct,
    d.rsi_14,
    d.macd_hist,
    d.cmo_10,
    d.cmo_monthly_10,
    d.cmo_monthly_above_25,
    d.sma_20,
    d.sma_50,
    d.sma_200,
    d.industry_momentum_avg,
    d.industry_momentum_pct_rank,
    s.sector_avg_score,
    s.sector_rank_in_market,
    COALESCE(m.macro_regime, d.market_regime)   AS macro_regime,
    d.market_regime,
    d.risk_on_score,
    m.composite_macro_score,
    m.credit_score,
    m.growth_score,
    m.dollar_score,
    d.benchmark_return_20d_pct,
    CASE
        WHEN d.total_score >= 70 AND (d.rank_delta IS NULL OR d.rank_delta <=  10) THEN 'strong'
        WHEN d.total_score >= 50 AND d.rank_delta IS NOT NULL AND d.rank_delta <= -20 THEN 'rising'
        WHEN d.total_score >= 50 AND d.rank_delta IS NOT NULL AND d.rank_delta >=  20 THEN 'fading'
        WHEN d.total_score  < 40                                                       THEN 'weak'
        ELSE 'neutral'
    END                 AS action_signal,
    d.is_rankable,
    d.is_valid_market_data,
    d.is_backtest_tradable,
    d.quality_reasons
FROM with_deltas AS d
LEFT JOIN sector_ctx AS s
    ON s.score_date = d.score_date AND s.market = d.market AND s.sector = d.sector
LEFT JOIN macro_ctx AS m
    ON m.ts = d.score_date
