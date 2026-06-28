{{ config(
    materialized='table',
) }}

-- Equity-only strategy variants.

WITH portfolio_sizes AS (
    SELECT unnest(ARRAY[10, 20, 40, 80]) AS portfolio_size
),

full_exposure AS (
    SELECT 'full' AS exposure_policy
),

base_frequencies AS (
    SELECT unnest(ARRAY['weekly', 'monthly', 'quarterly']) AS rebalance_frequency
),

academic_frequencies AS (
    SELECT unnest(ARRAY['weekly', 'monthly', 'quarterly', '6m']) AS rebalance_frequency
),

variant_grid AS (
    SELECT
        'momentum_base' AS strategy_family,
        f.rebalance_frequency,
        'equity' AS universe_scope,
        e.exposure_policy,
        p.portfolio_size,
        false AS requires_cmo_monthly
    FROM base_frequencies AS f
    CROSS JOIN full_exposure AS e
    CROSS JOIN portfolio_sizes AS p

    UNION ALL

    SELECT
        'momentum_academic' AS strategy_family,
        f.rebalance_frequency,
        'equity' AS universe_scope,
        e.exposure_policy,
        p.portfolio_size,
        false AS requires_cmo_monthly
    FROM academic_frequencies AS f
    CROSS JOIN full_exposure AS e
    CROSS JOIN portfolio_sizes AS p

    UNION ALL

    SELECT
        'momentum_academic_cmo' AS strategy_family,
        'monthly' AS rebalance_frequency,
        'equity' AS universe_scope,
        e.exposure_policy,
        p.portfolio_size,
        true AS requires_cmo_monthly
    FROM full_exposure AS e
    CROSS JOIN portfolio_sizes AS p

    UNION ALL

    SELECT
        'momentum_risk_adjusted' AS strategy_family,
        f.rebalance_frequency,
        'equity' AS universe_scope,
        e.exposure_policy,
        p.portfolio_size,
        false AS requires_cmo_monthly
    FROM academic_frequencies AS f
    CROSS JOIN full_exposure AS e
    CROSS JOIN portfolio_sizes AS p
)

SELECT
    concat(
        strategy_family,
        '_',
        rebalance_frequency,
        '_top',
        portfolio_size::text
    ) AS backtest_id,
    strategy_family,
    rebalance_frequency,
    portfolio_size,
    universe_scope,
    exposure_policy,
    requires_cmo_monthly
FROM variant_grid
