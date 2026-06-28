SELECT market
FROM {{ ref('stg_yahoo__equity_ohlcv_daily') }}
WHERE lower(market) NOT IN ('us', 'germany')

UNION ALL

SELECT market
FROM {{ ref('stg_yahoo__ticker_info') }}
WHERE lower(market) NOT IN ('us', 'germany')
