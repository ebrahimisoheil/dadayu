-- Fails when fewer than 120 DE tickers are currently active members.
SELECT count(*) AS active_de
FROM {{ ref('int_universe_membership_daily') }}
WHERE market = 'germany' AND valid_to IS NULL
HAVING count(*) < 120
