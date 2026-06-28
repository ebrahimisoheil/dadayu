-- Fails when fewer than 450 US tickers are currently active members.
SELECT count(*) AS active_us
FROM {{ ref('int_universe_membership_daily') }}
WHERE market = 'us' AND valid_to IS NULL
HAVING count(*) < 450
