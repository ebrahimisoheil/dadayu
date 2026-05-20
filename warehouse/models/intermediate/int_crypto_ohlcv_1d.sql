{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree()',
    order_by='(ticker, market, ts)',
    partition_by='toYYYYMM(ts)',
    unique_key=['ticker', 'market', 'ts'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

SELECT
    ticker,
    market,
    ts,
    open,
    high,
    low,
    close,
    volume
FROM {{ ref('stg_yahoo__crypto_ohlcv_1d') }}
{% if is_incremental() %}
WHERE ts > (SELECT max(ts) FROM {{ this }})
{% endif %}
