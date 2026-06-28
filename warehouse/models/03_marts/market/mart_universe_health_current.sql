{{ config(
    materialized='table',
) }}

WITH price_summary AS (
    SELECT
        ticker,
        market,
        count(*) AS price_history_rows,
        min(date) AS first_price_date,
        max(date) AS last_price_date,
        (array_agg(close ORDER BY date DESC))[1] AS last_close
    FROM {{ source('yahoo', 'prices_daily') }}
    GROUP BY ticker, market
),

latest_score_date AS (
    SELECT max(score_date) AS score_date
    FROM {{ ref('mart_portfolio_asset_scores_daily') }}
),

latest_scores AS (
    SELECT
        s.ticker,
        s.market,
        s.score_date,
        s.total_score,
        s.overall_rank,
        s.is_rankable,
        s.is_backtest_tradable,
        s.quality_reasons
    FROM {{ ref('mart_portfolio_asset_scores_daily') }} AS s
    INNER JOIN latest_score_date AS d
        ON s.score_date = d.score_date
),

flags AS (
    SELECT
        t.ticker,
        t.market,
        t.name,
        t.sector,
        t.industry,
        t.currency,
        t.country,
        t.market_cap,
        t.pe_ratio,
        t.fetched_at,
        p.price_history_rows,
        p.first_price_date,
        p.last_price_date,
        p.last_close,
        s.score_date AS latest_score_date,
        s.total_score,
        s.overall_rank,
        coalesce(s.is_rankable, false) AS is_rankable,
        coalesce(s.is_backtest_tradable, false) AS is_backtest_tradable,
        s.quality_reasons,
        p.price_history_rows IS NULL AS missing_price_history,
        p.last_price_date IS NOT NULL
            AND p.last_price_date < current_date - INTERVAL '5 days' AS stale_price_history,
        nullif(t.sector, '') IS NULL AS missing_sector,
        nullif(t.industry, '') IS NULL AS missing_industry,
        t.market_cap IS NULL AS missing_market_cap
    FROM {{ source('yahoo', 'tickers') }} AS t
    LEFT JOIN price_summary AS p
        ON t.ticker = p.ticker
        AND t.market = p.market
    LEFT JOIN latest_scores AS s
        ON t.ticker = s.ticker
        AND t.market = s.market
)

SELECT
    *,
    CASE
        WHEN missing_price_history THEN 'no_price_history'
        WHEN stale_price_history THEN 'stale_price_history'
        WHEN missing_sector OR missing_industry THEN 'missing_sector_or_industry'
        WHEN missing_market_cap THEN 'missing_market_cap'
        WHEN NOT is_rankable THEN 'not_rankable_latest'
        ELSE 'ok'
    END AS universe_status,
    array_to_string(array_remove(ARRAY[
        CASE WHEN missing_price_history THEN 'no_price_history' ELSE '' END,
        CASE WHEN stale_price_history THEN 'stale_price_history' ELSE '' END,
        CASE WHEN missing_sector THEN 'missing_sector' ELSE '' END,
        CASE WHEN missing_industry THEN 'missing_industry' ELSE '' END,
        CASE WHEN missing_market_cap THEN 'missing_market_cap' ELSE '' END,
        CASE WHEN NOT is_rankable THEN 'not_rankable_latest' ELSE '' END,
        coalesce(nullif(quality_reasons, ''), '')
    ], ''), '|') AS universe_reasons,
    NOT missing_price_history
        AND NOT stale_price_history
        AND NOT missing_sector
        AND NOT missing_industry
        AND NOT missing_market_cap
        AND is_rankable AS is_product_eligible
FROM flags
