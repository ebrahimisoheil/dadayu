{{ config(
    materialized='table',
) }}

SELECT * FROM {{ ref('int_macro_regime_daily') }}
