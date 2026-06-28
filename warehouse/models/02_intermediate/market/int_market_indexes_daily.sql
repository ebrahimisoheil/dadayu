{{ config(
    materialized='table',
) }}

WITH index_prices AS (
    SELECT
        i.index_id,
        p.ticker,
        p.market,
        i.name,
        i.region,
        i.benchmark_type,
        p.ts,
        p.open,
        p.high,
        p.low,
        p.close,
        p.volume
    FROM {{ ref('stg_yahoo__index_ohlcv_daily') }} AS p
    INNER JOIN {{ ref('index_universe') }} AS i
        ON p.ticker = i.ticker
        AND p.market = i.market
)

SELECT
    *,
    (close - coalesce(lag(close, 1) OVER w, close))
        / nullif(coalesce(lag(close, 1) OVER w, close), 0) AS return_pct,
    ((close / nullif(lag(close, 20) OVER w, 0)) - 1) * 100 AS return_20d_pct,
    avg(close) OVER (
        PARTITION BY index_id
        ORDER BY ts
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) AS sma_50,
    avg(close) OVER (
        PARTITION BY index_id
        ORDER BY ts
        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
    ) AS sma_200
FROM index_prices
WINDOW w AS (PARTITION BY index_id ORDER BY ts)
