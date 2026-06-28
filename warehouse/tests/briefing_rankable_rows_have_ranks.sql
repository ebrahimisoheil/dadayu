SELECT
    score_date,
    ticker,
    market
FROM {{ ref('mart_briefing_portfolio_daily') }}
WHERE is_rankable
  AND (
    overall_rank IS NULL
    OR risk_rank IS NULL
    OR risk_bucket IS NULL
    OR risk_bucket = ''
  )
