{{ config(
    materialized='table',
) }}

WITH enriched AS (
    SELECT
        b.*,
        s.sector_rank_in_market,
        s.breadth_above_sma_200_pct AS sector_breadth_above_sma_200_pct,
        i.industry_rank_in_sector,
        i.industry_rank_in_market,
        i.breadth_above_sma_200_pct AS industry_breadth_above_sma_200_pct
    FROM {{ ref('mart_briefing_portfolio_daily') }} AS b
    LEFT JOIN {{ ref('mart_sector_scores_daily') }} AS s
        ON b.score_date = s.score_date
        AND b.market = s.market
        AND coalesce(nullif(b.sector, ''), 'Unknown') = s.sector
    LEFT JOIN {{ ref('mart_industry_scores_daily') }} AS i
        ON b.score_date = i.score_date
        AND b.market = i.market
        AND coalesce(nullif(b.sector, ''), 'Unknown') = i.sector
        AND coalesce(nullif(b.industry, ''), 'Unknown') = i.industry
),

ranked AS (
    SELECT
        *,
        rank() OVER (
            PARTITION BY score_date, market
            ORDER BY total_score DESC NULLS LAST, momentum_12_1_pct DESC NULLS LAST
        ) AS product_rank
    FROM enriched
    WHERE is_rankable
      AND is_backtest_tradable
)

SELECT
    *,
    CASE
        WHEN product_rank <= 10 THEN 'top_10'
        WHEN product_rank <= 20 THEN 'top_20'
        WHEN product_rank <= 30 THEN 'top_30'
        WHEN product_rank <= 100 THEN 'watchlist'
        ELSE 'long_tail'
    END AS rank_list_bucket,
    CASE
        WHEN product_rank <= 30 THEN 'buy_candidate'
        WHEN product_rank <= 100 THEN 'watch'
        WHEN total_score < 35 THEN 'avoid'
        ELSE 'hold'
    END AS action_bucket,
    CASE
        WHEN risk_bucket = 'high' THEN 'high_volatility'
        WHEN coalesce(sector_rank_in_market, 999) <= 3 THEN 'strong_sector_context'
        WHEN coalesce(industry_rank_in_sector, 999) <= 3 THEN 'strong_industry_context'
        ELSE 'score_driven'
    END AS primary_signal_reason,
    CASE
        WHEN risk_bucket = 'high' THEN 'Higher volatility than sector peers.'
        WHEN coalesce(sector_breadth_above_sma_200_pct, 0) < 40 THEN 'Sector breadth is weak.'
        WHEN coalesce(industry_breadth_above_sma_200_pct, 0) < 40 THEN 'Industry breadth is weak.'
        ELSE 'No major model risk flag.'
    END AS risk_note,
    'Informational ranking only, not investment advice.' AS product_disclaimer
FROM ranked
