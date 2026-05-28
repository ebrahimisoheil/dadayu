{{ config(
    materialized='table',
) }}

SELECT
    ts,
    composite_macro_score,
    AVG(composite_macro_score)
        OVER (ORDER BY ts ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
        AS composite_macro_score_30d_avg,
    composite_macro_score
        - AVG(composite_macro_score)
            OVER (ORDER BY ts ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
        AS composite_macro_score_trend,
    credit_score,
    rates_score,
    inflation_score,
    dollar_score,
    growth_score,
    sector_score,
    macro_regime,
    market_regime,
    risk_on_score,
    benchmark_close,
    benchmark_return_pct,
    benchmark_above_sma_200,
    -- Hysteresis constants stored here so downstream consumers don't hardcode thresholds
    70.0 AS risk_on_entry_threshold,
    55.0 AS risk_on_exit_threshold,
    30.0 AS risk_off_entry_threshold,
    45.0 AS risk_off_exit_threshold
FROM {{ ref('int_macro_regime_daily') }}
ORDER BY ts
