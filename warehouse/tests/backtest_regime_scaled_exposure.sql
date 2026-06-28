SELECT
    backtest_id,
    equity_date,
    exposure_policy,
    market_regime,
    exposure_multiplier
FROM {{ ref('mart_backtest_portfolio_equity_daily') }}
WHERE (exposure_policy = 'full' AND abs(exposure_multiplier - 1.0) > 0.000001)
   OR (
        exposure_policy = 'regime_scaled'
        AND exposure_multiplier NOT IN (1.0, 0.7, 0.3)
   )
   OR exposure_policy NOT IN ('full', 'regime_scaled')
