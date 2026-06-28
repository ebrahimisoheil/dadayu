{{ config(
    materialized='table',
) }}

WITH base AS (
    SELECT
        ticker,
        market,
        asset_type,
        ts,
        open,
        high,
        low,
        close,
        volume,
        return_pct,
        dollar_volume,
        avg_dollar_volume_20d,
        max_abs_return_5d,
        unchanged_close_count_5d,
        has_positive_prices,
        has_positive_volume,
        has_valid_ohlc,
        has_extreme_return,
        has_stale_price,
        is_low_liquidity,
        is_valid_market_data,
        is_backtest_tradable,
        quality_reasons
    FROM {{ ref('int_market_data_quality_daily') }}
)

SELECT * FROM base
