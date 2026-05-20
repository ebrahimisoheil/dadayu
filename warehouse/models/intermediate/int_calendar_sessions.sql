{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(date, market)'
) }}

SELECT
    date,
    market,
    is_trading_day,
    session_open_utc,
    session_close_utc,
    concat(upper(market), '_', toString(toYYYYMMDD(date))) AS session_id
FROM {{ ref('trading_calendar') }}
