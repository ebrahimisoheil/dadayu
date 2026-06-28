{{ config(
    materialized='table',
) }}

SELECT
    s.ticker,
    s.market,
    s.asset_type,
    s.score_date,
    p.close AS close,
    p.avg_dollar_volume_20d AS avg_dollar_volume_20d,
    s.total_score AS base_total_score,
    s.momentum_score,
    s.cmo_score,
    s.rsi_score,
    s.macd_score,
    s.trend_score,
    s.momentum_12_1_pct,
    s.momentum_rank_in_market,
    s.cmo_10,
    s.cmo_monthly_10,
    s.cmo_monthly_above_25,
    s.rsi_14,
    s.macd_hist,
    s.atr_pct,
    s.risk_bucket,
    s.is_rankable
FROM {{ ref('mart_portfolio_asset_scores_daily') }} AS s
INNER JOIN {{ ref('int_market_backtest_prices_daily') }} AS p
    ON s.ticker = p.ticker
    AND s.market = p.market
    AND s.score_date = p.ts
WHERE s.score_date >= (current_date - INTERVAL '5 years')::timestamp
  AND s.score_date < current_date::timestamp
  AND s.is_rankable
  AND p.is_backtest_tradable
