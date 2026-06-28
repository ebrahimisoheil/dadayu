{{ config(
    materialized='table',
) }}

WITH weekly_dates AS (
    SELECT
        date_trunc('week', score_date)::timestamp AS week_start,
        max(score_date) AS weekly_score_date
    FROM {{ ref('mart_portfolio_asset_scores_daily') }}
    WHERE is_rankable
    GROUP BY week_start
),

weekly_scores AS (
    SELECT
        w.week_start,
        s.*
    FROM {{ ref('mart_portfolio_asset_scores_daily') }} AS s
    INNER JOIN weekly_dates AS w
        ON s.score_date = w.weekly_score_date
    WHERE s.is_rankable
)

SELECT
    week_start,
    score_date,
    ticker,
    market,
    asset_type,
    name,
    sector,
    industry,
    close,
    total_score,
    momentum_score,
    cmo_score,
    rsi_score,
    macd_score,
    trend_score,
    momentum_12_1_pct,
    momentum_rank_in_market,
    rank() OVER (
        PARTITION BY week_start
        ORDER BY total_score DESC
    ) AS overall_rank,
    rank() OVER (
        PARTITION BY week_start, market
        ORDER BY total_score DESC
    ) AS market_rank
FROM weekly_scores

