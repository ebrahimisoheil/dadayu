{% macro ema(col, n) %}
    {# ClickHouse has no native EMA window function; use arrayFold over grouped array then rejoin #}
    arrayFold(
        (acc, x) -> acc + (2.0 / ({{ n }} + 1)) * (x - acc),
        groupArray({{ col }}) OVER (
            PARTITION BY ticker, market
            ORDER BY ts
            ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
        ),
        toFloat64(0)
    )
{% endmacro %}
