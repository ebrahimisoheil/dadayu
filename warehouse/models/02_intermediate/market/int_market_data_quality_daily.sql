{{ config(
    materialized='table',
) }}

WITH prepped AS (
    SELECT
        *,
        close * volume AS dollar_volume,
        lag(close, 1) OVER w AS previous_close
    FROM {{ ref('int_market_assets_daily') }}
    WINDOW w AS (
        PARTITION BY ticker, market
        ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
),

scored AS (
    SELECT
        *,
        avg(dollar_volume) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_dollar_volume_20d,
        max(abs(coalesce(return_pct, 0))) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS max_abs_return_5d,
        sum(CASE WHEN previous_close IS NOT NULL AND close = previous_close THEN 1 ELSE 0 END) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS unchanged_close_count_5d
    FROM prepped
),

flags AS (
    SELECT
        *,
        open > 0
            AND high > 0
            AND low > 0
            AND close > 0 AS has_positive_prices,
        volume > 0 AS has_positive_volume,
        high >= low
            AND high >= greatest(open, close)
            AND low <= least(open, close) AS has_valid_ohlc,
        abs(coalesce(return_pct, 0)) > 1.0
            OR max_abs_return_5d > 1.0 AS has_extreme_return,
        unchanged_close_count_5d >= 5 AS has_stale_price,
        CASE
            WHEN asset_type = 'equity' THEN close < 5 OR avg_dollar_volume_20d < 5000000
            ELSE true
        END AS is_low_liquidity
    FROM scored
)

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
    has_positive_prices
        AND has_positive_volume
        AND has_valid_ohlc
        AND NOT has_extreme_return
        AND NOT has_stale_price AS is_valid_market_data,
    has_positive_prices
        AND has_positive_volume
        AND has_valid_ohlc
        AND NOT has_extreme_return
        AND NOT has_stale_price
        AND NOT is_low_liquidity AS is_backtest_tradable,
    array_to_string(array_remove(ARRAY[
        CASE WHEN NOT has_positive_prices THEN 'non_positive_price' ELSE '' END,
        CASE WHEN NOT has_positive_volume THEN 'non_positive_volume' ELSE '' END,
        CASE WHEN NOT has_valid_ohlc THEN 'invalid_ohlc' ELSE '' END,
        CASE WHEN has_extreme_return THEN 'extreme_return' ELSE '' END,
        CASE WHEN has_stale_price THEN 'stale_price' ELSE '' END,
        CASE WHEN is_low_liquidity THEN 'low_liquidity' ELSE '' END
    ], ''), '|') AS quality_reasons
FROM flags
