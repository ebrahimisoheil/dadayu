{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(date, market)'
) }}

SELECT
    date,
    market,
    session_id,
    is_trading_day,
    session_open_utc,
    session_close_utc,
    toYear(date)                                AS year,
    toMonth(date)                               AS month,
    toISOWeek(date)                             AS week_of_year,
    toDayOfWeek(date)                           AS day_of_week,
    dateName('day', date)                       AS day_name
FROM {{ ref('int_calendar_sessions') }}
