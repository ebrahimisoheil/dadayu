{{ config(
    materialized='table',
) }}

WITH base AS (
    SELECT
        score_date,
        market,
        coalesce(nullif(sector, ''), 'Unknown') AS sector,
        ticker,
        name,
        close,
        sma_200,
        total_score,
        momentum_12_1_pct,
        atr_pct,
        risk_bucket,
        is_rankable,
        is_backtest_tradable
    FROM {{ ref('mart_portfolio_asset_scores_daily') }}
),

aggregated AS (
    SELECT
        score_date,
        market,
        sector,
        count(*) AS asset_count,
        count(*) FILTER (WHERE is_rankable) AS rankable_count,
        count(*) FILTER (WHERE is_backtest_tradable) AS tradable_count,
        round(avg(total_score) FILTER (WHERE is_rankable)::numeric, 2) AS avg_total_score,
        round(percentile_cont(0.5) WITHIN GROUP (ORDER BY total_score) FILTER (WHERE is_rankable)::numeric, 2) AS median_total_score,
        round(avg(momentum_12_1_pct) FILTER (WHERE is_rankable)::numeric, 4) AS avg_momentum_12_1_pct,
        round(avg(atr_pct) FILTER (WHERE is_rankable)::numeric, 4) AS avg_atr_pct,
        count(*) FILTER (WHERE is_rankable AND close > sma_200) AS above_sma_200_count,
        count(*) FILTER (WHERE is_rankable AND risk_bucket = 'high') AS high_risk_count,
        (array_agg(ticker ORDER BY total_score DESC NULLS LAST) FILTER (WHERE is_rankable))[1] AS top_ticker,
        (array_agg(name ORDER BY total_score DESC NULLS LAST) FILTER (WHERE is_rankable))[1] AS top_name,
        (array_agg(total_score ORDER BY total_score DESC NULLS LAST) FILTER (WHERE is_rankable))[1] AS top_total_score
    FROM base
    GROUP BY score_date, market, sector
),

scored AS (
    SELECT
        *,
        round(above_sma_200_count::numeric / nullif(rankable_count, 0) * 100, 2) AS breadth_above_sma_200_pct,
        round(high_risk_count::numeric / nullif(rankable_count, 0) * 100, 2) AS high_risk_pct
    FROM aggregated
)

SELECT
    *,
    rank() OVER (
        PARTITION BY score_date
        ORDER BY avg_total_score DESC NULLS LAST, rankable_count DESC, sector ASC
    ) AS sector_rank_overall,
    rank() OVER (
        PARTITION BY score_date, market
        ORDER BY avg_total_score DESC NULLS LAST, rankable_count DESC, sector ASC
    ) AS sector_rank_in_market
FROM scored
