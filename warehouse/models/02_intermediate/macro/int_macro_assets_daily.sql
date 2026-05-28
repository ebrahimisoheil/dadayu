{{ config(
    materialized='table',
) }}

WITH base AS (
    SELECT
        ticker,
        ts,
        close,
        instrument_type,
        regime_dimension,
        LAG(close, 1)  OVER (PARTITION BY ticker ORDER BY ts) AS prev_close_1d,
        LAG(close, 20) OVER (PARTITION BY ticker ORDER BY ts) AS prev_close_20d,
        LAG(close, 60) OVER (PARTITION BY ticker ORDER BY ts) AS prev_close_60d,
        AVG(close) OVER (PARTITION BY ticker ORDER BY ts ROWS BETWEEN 19  PRECEDING AND CURRENT ROW) AS sma_20,
        AVG(close) OVER (PARTITION BY ticker ORDER BY ts ROWS BETWEEN 49  PRECEDING AND CURRENT ROW) AS sma_50,
        AVG(close) OVER (PARTITION BY ticker ORDER BY ts ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma_200
    FROM {{ ref('stg_yahoo__macro_ohlcv_daily') }}
    WHERE close > 0
)

SELECT
    ticker,
    ts,
    close,
    instrument_type,
    regime_dimension,
    CASE WHEN prev_close_1d  IS NOT NULL AND prev_close_1d  <> 0
         THEN (close / prev_close_1d  - 1) * 100 END AS return_pct,
    CASE WHEN prev_close_20d IS NOT NULL AND prev_close_20d <> 0
         THEN (close / prev_close_20d - 1) * 100 END AS return_20d_pct,
    CASE WHEN prev_close_60d IS NOT NULL AND prev_close_60d <> 0
         THEN (close / prev_close_60d - 1) * 100 END AS return_60d_pct,
    sma_20,
    sma_50,
    sma_200,
    CASE WHEN close > sma_20  THEN 1 ELSE 0 END AS above_sma_20,
    CASE WHEN close > sma_50  THEN 1 ELSE 0 END AS above_sma_50,
    CASE WHEN close > sma_200 THEN 1 ELSE 0 END AS above_sma_200,
    0.0 AS pct_rank_return_20d
FROM base
