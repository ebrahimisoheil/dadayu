{{ config(
    materialized='table',
) }}

WITH asset_scores_base AS (
    SELECT
        ticker,
        market,
        asset_type,
        score_date AS signal_date,
        close,
        avg_dollar_volume_20d,
        base_total_score,
        momentum_score,
        cmo_score,
        rsi_score,
        macd_score,
        trend_score,
        momentum_12_1_pct,
        momentum_rank_in_market,
        cmo_10,
        cmo_monthly_10,
        cmo_monthly_above_25,
        rsi_14,
        macd_hist,
        atr_pct,
        risk_bucket,
        is_rankable
    FROM {{ ref('int_backtest_asset_scores_daily') }}
),

asset_scores AS (
    SELECT * FROM asset_scores_base
),

forward_prices AS (
    SELECT ticker, market, signal_date, close, close_5d_forward, close_20d_forward, close_60d_forward
    FROM {{ ref('int_market_forward_prices_daily') }}
),

family_candidates AS (
    SELECT
        'momentum_base' AS strategy_family,
        s.*,
        s.base_total_score AS strategy_score
    FROM asset_scores AS s

    UNION ALL

    SELECT
        'momentum_academic' AS strategy_family,
        s.*,
        s.momentum_12_1_pct AS strategy_score
    FROM asset_scores AS s

    UNION ALL

    SELECT
        'momentum_academic_cmo' AS strategy_family,
        s.*,
        s.momentum_12_1_pct AS strategy_score
    FROM asset_scores AS s
    WHERE s.cmo_monthly_above_25

    UNION ALL

    SELECT
        'momentum_risk_adjusted' AS strategy_family,
        s.*,
        s.momentum_12_1_pct / nullif(greatest(coalesce(s.atr_pct, 0), 0.01), 0) AS strategy_score
    FROM asset_scores AS s
),

scoped_candidates AS (
    SELECT
        c.*,
        'equity' AS universe_scope
    FROM family_candidates AS c
    INNER JOIN {{ ref('int_universe_membership_daily') }} AS m
        ON c.ticker = m.ticker
        AND c.market = m.market
        AND c.signal_date >= m.valid_from
        AND (m.valid_to IS NULL OR c.signal_date < m.valid_to)
    WHERE c.asset_type = 'equity'
),

top_limited_candidates AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY strategy_family, universe_scope, signal_date
                ORDER BY strategy_score DESC, ticker ASC, market ASC
            ) AS pre_limit_rank
        FROM scoped_candidates
        WHERE strategy_score IS NOT NULL
    ) AS limited
    WHERE pre_limit_rank <= 80
),

ranked AS (
    SELECT
        c.*,
        row_number() OVER (
            PARTITION BY strategy_family, universe_scope, signal_date, market
            ORDER BY strategy_score DESC, ticker ASC, market ASC
        ) AS signal_rank_in_market,
        row_number() OVER (
            PARTITION BY strategy_family, universe_scope, signal_date
            ORDER BY strategy_score DESC, ticker ASC, market ASC
        ) AS signal_rank
    FROM top_limited_candidates AS c
),

variant_candidates AS (
    SELECT
        v.backtest_id,
        v.rebalance_frequency,
        v.portfolio_size,
        v.exposure_policy,
        c.*
    FROM ranked AS c
    INNER JOIN {{ ref('mart_backtest_strategy_variants') }} AS v
        ON c.strategy_family = v.strategy_family
        AND c.universe_scope = v.universe_scope
    WHERE c.signal_rank <= v.portfolio_size
),

with_returns AS (
    SELECT
        v.*,
        f.close_5d_forward,
        f.close_20d_forward,
        f.close_60d_forward,
        ((f.close_5d_forward / nullif(v.close, 0)) - 1) * 100 AS return_5d,
        ((f.close_20d_forward / nullif(v.close, 0)) - 1) * 100 AS return_20d,
        ((f.close_60d_forward / nullif(v.close, 0)) - 1) * 100 AS return_60d
    FROM variant_candidates AS v
    LEFT JOIN forward_prices AS f
        ON v.ticker = f.ticker
        AND v.market = f.market
        AND v.signal_date = f.signal_date
)

SELECT
    backtest_id,
    strategy_family,
    rebalance_frequency,
    portfolio_size,
    universe_scope,
    exposure_policy,
    signal_date,
    ticker,
    market,
    asset_type,
    close,
    avg_dollar_volume_20d,
    base_total_score,
    strategy_score AS total_score,
    momentum_score,
    cmo_score,
    rsi_score,
    macd_score,
    trend_score,
    momentum_12_1_pct,
    momentum_rank_in_market,
    cmo_10,
    cmo_monthly_10,
    cmo_monthly_above_25,
    rsi_14,
    macd_hist,
    atr_pct,
    risk_bucket,
    signal_rank,
    signal_rank_in_market,
    close_5d_forward,
    close_20d_forward,
    close_60d_forward,
    return_5d,
    return_20d,
    return_60d
FROM with_returns
