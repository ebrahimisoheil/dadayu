SELECT
    backtest_id,
    count(*) AS total_rows,
    count(*) FILTER (WHERE return_20d IS NOT NULL) AS rows_with_20d_return
FROM {{ ref('mart_backtest_signals_daily') }}
WHERE signal_date < (current_date - INTERVAL '40 days')::timestamp
GROUP BY backtest_id
HAVING count(*) FILTER (WHERE return_20d IS NOT NULL) = 0
