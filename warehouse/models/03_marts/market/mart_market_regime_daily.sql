{{ config(
    materialized='table',
) }}

SELECT *
FROM {{ ref('int_market_regime_daily') }}

