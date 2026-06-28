{{ config(
    materialized='table',
) }}

WITH ranked AS (
    SELECT
        i.ticker,
        i.market,
        i.asset_type,
        i.ts AS score_date,
        i.name,
        i.sector,
        i.industry,
        i.close,
        i.sma_20,
        i.sma_50,
        i.sma_200,
        i.rsi_14,
        i.macd_hist,
        i.atr_14,
        i.cmo_10,
        i.cmo_monthly_10,
        i.cmo_monthly_above_25,
        i.momentum_12_1_pct,
        i.has_12_1_history,
        i.has_trend_history,
        i.has_minimum_history,
        coalesce(q.is_valid_market_data, false) AS is_valid_market_data,
        coalesce(q.is_backtest_tradable, false) AS is_backtest_tradable,
        q.quality_reasons AS quality_reasons,
        rank() OVER (
            PARTITION BY i.ts, i.market
            ORDER BY i.momentum_12_1_pct DESC
        ) AS momentum_rank_in_market
    FROM {{ ref('int_market_asset_indicators_daily') }} AS i
    LEFT JOIN {{ ref('int_market_data_quality_daily') }} AS q
        ON i.ticker = q.ticker
        AND i.market = q.market
        AND i.ts = q.ts
),

scored AS (
    SELECT
        *,
        CASE
            WHEN NOT has_12_1_history THEN NULL
            WHEN momentum_rank_in_market <= 20 THEN 100
            WHEN momentum_rank_in_market <= 50 THEN 80
            WHEN momentum_rank_in_market <= 100 THEN 60
            WHEN momentum_rank_in_market <= 200 THEN 40
            ELSE 20
        END AS momentum_score,
        CASE
            WHEN cmo_10 > 25 THEN 80
            WHEN cmo_10 > 0 THEN 50
            ELSE 20
        END AS cmo_score,
        CASE
            WHEN rsi_14 < 30 THEN 90
            WHEN rsi_14 < 40 THEN 70
            WHEN rsi_14 < 60 THEN 50
            WHEN rsi_14 < 70 THEN 30
            ELSE 10
        END AS rsi_score,
        CASE
            WHEN macd_hist > 0 THEN 80
            ELSE 20
        END AS macd_score,
        CASE
            WHEN close > sma_20 AND sma_20 > sma_50 AND sma_50 > sma_200 THEN 100
            WHEN close > sma_20 AND sma_20 > sma_50 THEN 80
            WHEN close > sma_20 THEN 60
            ELSE 30
        END AS trend_score
    FROM ranked
),

with_total AS (
    SELECT
        *,
        has_12_1_history
            AND has_trend_history
            AND has_minimum_history
            AND is_valid_market_data AS is_rankable,
        atr_14 / nullif(close, 0) AS atr_pct,
        CASE
            WHEN has_12_1_history AND has_trend_history AND has_minimum_history THEN
                momentum_score * 0.35
                + cmo_score * 0.20
                + rsi_score * 0.15
                + macd_score * 0.15
                + trend_score * 0.15
            ELSE NULL
        END AS total_score
    FROM scored
),

sector_counts AS (
    SELECT
        score_date,
        sector,
        count(*) FILTER (WHERE is_rankable AND atr_pct IS NOT NULL) AS sector_rankable_count
    FROM with_total
    GROUP BY score_date, sector
),

risk_partitioned AS (
    SELECT
        t.score_date,
        t.ticker,
        t.market,
        t.atr_pct,
        CASE
            WHEN coalesce(c.sector_rankable_count, 0) < 5
                OR coalesce(t.sector, '') = ''
                THEN concat('market:', t.market)
            ELSE concat('sector:', t.sector)
        END AS volatility_partition_key
    FROM with_total AS t
    LEFT JOIN sector_counts AS c
        ON t.score_date = c.score_date
        AND t.sector = c.sector
    WHERE t.is_rankable
      AND t.atr_pct IS NOT NULL
),

volatility_ranked AS (
    SELECT
        score_date,
        ticker,
        market,
        percent_rank() OVER (
            PARTITION BY score_date, volatility_partition_key
            ORDER BY atr_pct ASC
        ) AS volatility_pct_in_sector
    FROM risk_partitioned
)

SELECT
    t.*,
    coalesce(v.volatility_pct_in_sector, 0) AS volatility_pct_in_sector,
    CASE
        WHEN coalesce(v.volatility_pct_in_sector, 0) >= 0.67 THEN 'high'
        WHEN coalesce(v.volatility_pct_in_sector, 0) >= 0.33 THEN 'medium'
        ELSE 'low'
    END AS risk_bucket,
    rank() OVER (
        PARTITION BY t.score_date
        ORDER BY t.total_score DESC
    ) AS overall_rank
FROM with_total AS t
LEFT JOIN volatility_ranked AS v
    ON t.score_date = v.score_date
    AND t.ticker = v.ticker
    AND t.market = v.market
