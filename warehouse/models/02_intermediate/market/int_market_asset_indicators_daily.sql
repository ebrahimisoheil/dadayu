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
        name,
        sector,
        industry,
        market_cap,
        row_number() OVER (PARTITION BY ticker, market ORDER BY ts) AS bars_available,
        coalesce(lag(close, 1) OVER (PARTITION BY ticker, market ORDER BY ts), close) AS prev_close
    FROM {{ ref('int_market_assets_daily') }}
),

prepped AS (
    SELECT
        *,
        CASE WHEN close > prev_close THEN close - prev_close ELSE 0.0 END AS gain,
        CASE WHEN close < prev_close THEN prev_close - close ELSE 0.0 END AS loss,
        greatest(high - low, abs(high - prev_close), abs(low - prev_close)) AS true_range,
        avg(close) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS ema_12,
        avg(close) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
            ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
        ) AS ema_26
    FROM base
),

indicators AS (
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
        name,
        sector,
        industry,
        market_cap,
        bars_available,
        {{ sma('close', 20) }} AS sma_20,
        {{ sma('close', 50) }} AS sma_50,
        {{ sma('close', 200) }} AS sma_200,
        {{ rsi('gain', 'loss', 14) }} AS rsi_14,
        ema_12 - ema_26 AS macd_line,
        avg(ema_12 - ema_26) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
            ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
        ) AS macd_signal,
        (ema_12 - ema_26) - avg(ema_12 - ema_26) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
            ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
        ) AS macd_hist,
        avg(true_range) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS atr_14,
        sum(gain) FILTER (WHERE gain > 0) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS cmo_sum_gains_10,
        sum(loss) FILTER (WHERE loss > 0) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS cmo_sum_losses_10,
        sum(gain) FILTER (WHERE gain > 0) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) AS cmo_monthly_gains,
        sum(loss) FILTER (WHERE loss > 0) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) AS cmo_monthly_losses,
        lag(close, 42) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
        ) AS signal_close_42d,
        lag(close, 273) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
        ) AS close_273d_ago
    FROM prepped
)

SELECT
    *,
    CASE
        WHEN cmo_sum_gains_10 + cmo_sum_losses_10 = 0 THEN 0
        ELSE ((cmo_sum_gains_10 - cmo_sum_losses_10) / (cmo_sum_gains_10 + cmo_sum_losses_10)) * 100
    END AS cmo_10,
    CASE
        WHEN cmo_monthly_gains + cmo_monthly_losses = 0 THEN 0
        ELSE ((cmo_monthly_gains - cmo_monthly_losses) / nullif(cmo_monthly_gains + cmo_monthly_losses, 0)) * 100
    END AS cmo_monthly_10,
    coalesce(
        CASE
            WHEN cmo_monthly_gains + cmo_monthly_losses = 0 THEN 0
            ELSE ((cmo_monthly_gains - cmo_monthly_losses) / nullif(cmo_monthly_gains + cmo_monthly_losses, 0)) * 100
        END > 25,
        false
    ) AS cmo_monthly_above_25,
    ((signal_close_42d / nullif(close_273d_ago, 0)) - 1) * 100 AS momentum_12_1_pct,
    bars_available >= 274 AS has_12_1_history,
    bars_available >= 200 AS has_trend_history,
    bars_available >= 60 AS has_minimum_history
FROM indicators
