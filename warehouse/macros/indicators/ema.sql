{% macro ema(col, n) %}
    {#
      True EMA seeded from first available price (sentinel -1 triggers init).
      Uses UNBOUNDED PRECEDING so the accumulator carries forward from bar 1,
      not from 0 over a fixed N-row window.
      Multiplier k = 2 / (n + 1).
    #}
    arrayFold(
        (acc, x) -> if(acc < 0, x, acc + (2.0 / ({{ n }} + 1)) * (x - acc)),
        groupArray({{ col }}) OVER (
            PARTITION BY ticker, market ORDER BY ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        todouble precision(-1)
    )
{% endmacro %}
