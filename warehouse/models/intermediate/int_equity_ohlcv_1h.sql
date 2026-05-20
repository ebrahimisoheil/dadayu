{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree()',
    order_by='(ticker, market, ts)',
    partition_by='toYYYYMM(ts)',
    unique_key=['ticker', 'market', 'ts'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

WITH ohlcv AS (
    SELECT * FROM {{ ref('stg_yahoo__ohlcv_1h') }}
    {% if is_incremental() %}
    WHERE ts > (SELECT max(ts) FROM {{ this }})
    {% endif %}
),

calendar AS (
    SELECT * FROM {{ ref('int_calendar_sessions') }}
)

SELECT
    o.ticker,
    o.market,
    o.ts,
    o.open,
    o.high,
    o.low,
    o.close,
    o.volume,
    c.session_id,
    c.is_trading_day,
    c.session_open_utc,
    c.session_close_utc
FROM ohlcv AS o
LEFT JOIN calendar AS c
    ON toDate(o.ts) = c.date
    AND o.market = c.market
