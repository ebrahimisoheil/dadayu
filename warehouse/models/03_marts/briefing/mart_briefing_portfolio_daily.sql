{{ config(
    materialized='table',
) }}

WITH base AS (
    SELECT
        p.ticker AS ticker,
        p.market AS market,
        p.asset_type AS asset_type,
        p.score_date AS score_date,
        p.name AS name,
        p.sector AS sector,
        p.industry AS industry,
        p.close AS close,
        p.momentum_12_1_pct AS momentum_12_1_pct,
        p.momentum_rank_in_market AS momentum_rank_in_market,
        p.cmo_10 AS cmo_10,
        p.cmo_monthly_10 AS cmo_monthly_10,
        p.cmo_monthly_above_25 AS cmo_monthly_above_25,
        p.rsi_14 AS rsi_14,
        p.macd_hist AS macd_hist,
        p.sma_20 AS sma_20,
        p.sma_50 AS sma_50,
        p.sma_200 AS sma_200,
        p.total_score AS total_score,
        p.momentum_score AS momentum_score,
        p.cmo_score AS cmo_score,
        p.rsi_score AS rsi_score,
        p.macd_score AS macd_score,
        p.trend_score AS trend_score,
        p.is_rankable AS is_rankable,
        p.is_valid_market_data AS is_valid_market_data,
        p.is_backtest_tradable AS is_backtest_tradable,
        p.quality_reasons AS quality_reasons,
        p.risk_bucket AS risk_bucket,
        p.atr_pct AS atr_pct,
        p.volatility_pct_in_sector AS volatility_pct_in_sector,
        r.market_regime AS market_regime,
        r.risk_on_score AS risk_on_score,
        r.benchmark_return_20d_pct AS benchmark_return_20d_pct
    FROM {{ ref('mart_portfolio_asset_scores_daily') }} AS p
    LEFT JOIN {{ ref('mart_market_regime_daily') }} AS r
        ON p.score_date = r.ts
),

rankable_ranks AS (
    SELECT
        b.score_date AS score_date,
        b.ticker AS ticker,
        b.market AS market,
        rank() OVER (
            PARTITION BY b.score_date
            ORDER BY b.total_score DESC
        ) AS overall_rank,
        rank() OVER (
            PARTITION BY b.score_date, b.risk_bucket
            ORDER BY b.total_score DESC
        ) AS risk_rank
    FROM base AS b
    WHERE b.is_rankable
),

industry_stats AS (
    SELECT
        b.score_date AS score_date,
        b.industry AS industry,
        avg(b.momentum_12_1_pct) AS industry_momentum_avg
    FROM base AS b
    WHERE b.is_rankable
      AND b.momentum_12_1_pct IS NOT NULL
    GROUP BY b.score_date, b.industry
),

industry_ranks AS (
    SELECT
        b.score_date AS score_date,
        b.ticker AS ticker,
        b.market AS market,
        percent_rank() OVER (
            PARTITION BY b.score_date, b.industry
            ORDER BY b.momentum_12_1_pct ASC
        ) AS industry_momentum_pct_rank
    FROM base AS b
    WHERE b.is_rankable
      AND b.momentum_12_1_pct IS NOT NULL
)

SELECT
    b.ticker AS ticker,
    b.market AS market,
    b.asset_type AS asset_type,
    b.score_date AS score_date,
    b.name AS name,
    b.sector AS sector,
    b.industry AS industry,
    b.close AS close,
    b.momentum_12_1_pct AS momentum_12_1_pct,
    b.momentum_rank_in_market AS momentum_rank_in_market,
    b.cmo_10 AS cmo_10,
    b.cmo_monthly_10 AS cmo_monthly_10,
    b.cmo_monthly_above_25 AS cmo_monthly_above_25,
    b.rsi_14 AS rsi_14,
    b.macd_hist AS macd_hist,
    b.sma_20 AS sma_20,
    b.sma_50 AS sma_50,
    b.sma_200 AS sma_200,
    b.total_score AS total_score,
    b.momentum_score AS momentum_score,
    b.cmo_score AS cmo_score,
    b.rsi_score AS rsi_score,
    b.macd_score AS macd_score,
    b.trend_score AS trend_score,
    b.is_rankable AS is_rankable,
    b.is_valid_market_data AS is_valid_market_data,
    b.is_backtest_tradable AS is_backtest_tradable,
    b.quality_reasons AS quality_reasons,
    b.risk_bucket AS risk_bucket,
    b.atr_pct AS atr_pct,
    b.volatility_pct_in_sector AS volatility_pct_in_sector,
    r.overall_rank AS overall_rank,
    r.risk_rank AS risk_rank,
    s.industry_momentum_avg AS industry_momentum_avg,
    ir.industry_momentum_pct_rank AS industry_momentum_pct_rank,
    b.market_regime AS market_regime,
    b.risk_on_score AS risk_on_score,
    b.benchmark_return_20d_pct AS benchmark_return_20d_pct
FROM base AS b
LEFT JOIN rankable_ranks AS r
    ON b.score_date = r.score_date
    AND b.ticker = r.ticker
    AND b.market = r.market
LEFT JOIN industry_stats AS s
    ON b.score_date = s.score_date
    AND b.industry = s.industry
LEFT JOIN industry_ranks AS ir
    ON b.score_date = ir.score_date
    AND b.ticker = ir.ticker
    AND b.market = ir.market
