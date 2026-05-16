{% macro sma(col, n) %}
    avg({{ col }}) OVER (
        PARTITION BY ticker, market
        ORDER BY ts
        ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}
