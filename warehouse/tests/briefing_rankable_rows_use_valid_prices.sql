SELECT *
FROM {{ ref('mart_briefing_portfolio_daily') }}
WHERE is_rankable
  AND NOT is_valid_market_data

