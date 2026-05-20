{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(ticker, market, ts)',
    partition_by='toYYYYMM(ts)',
    pre_hook="SET max_bytes_before_external_sort = 2000000000, max_bytes_before_external_group_by = 2000000000"
) }}

WITH base AS (
    SELECT
        ticker,
        market,
        ts,
        open,
        high,
        low,
        close,
        lagInFrame(close, 1, close) OVER (PARTITION BY ticker, market ORDER BY ts) AS prev_close
    FROM {{ ref('fct_ohlcv_4h') }}
),

prepped AS (
    SELECT
        *,
        if(close > prev_close, close - prev_close, 0.0)                    AS gain,
        if(close < prev_close, prev_close - close, 0.0)                    AS loss,
        greatest(high - low, abs(high - prev_close), abs(low - prev_close)) AS true_range,
        {{ ema('close', 12) }}                                              AS ema_12,
        {{ ema('close', 26) }}                                              AS ema_26
    FROM base
)

SELECT
    ticker,
    market,
    ts,
    close,
    {{ sma('close', 20) }}                      AS sma_20,
    {{ ema('close', 20) }}                      AS ema_20,
    {{ rsi('gain', 'loss', 14) }}               AS rsi_14,
    {{ macd_line('ema_12', 'ema_26') }}         AS macd_line,
    {{ macd_signal('ema_12', 'ema_26', 9) }}    AS macd_signal,
    {{ macd_hist('ema_12', 'ema_26', 9) }}      AS macd_hist,
    {{ atr('true_range', 14) }}                 AS atr_14,
    {{ bb_upper('close', 20) }}                 AS bb_upper,
    {{ bb_middle('close', 20) }}                AS bb_middle,
    {{ bb_lower('close', 20) }}                 AS bb_lower
FROM prepped
