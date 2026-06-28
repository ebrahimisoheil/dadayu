SELECT
    score_date,
    ticker,
    market,
    count(*) AS row_count
FROM {{ ref('mart_briefing_portfolio_daily') }}
GROUP BY score_date, ticker, market
HAVING count(*) > 1
