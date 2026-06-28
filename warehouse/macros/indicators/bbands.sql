{% macro bb_middle(col, n) %}
    avg({{ col }}) OVER (
        PARTITION BY ticker, market ORDER BY ts
        ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

{% macro bb_upper(col, n, k=2) %}
    avg({{ col }}) OVER (
        PARTITION BY ticker, market ORDER BY ts
        ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
    ) + {{ k }} * stddev_pop({{ col }}) OVER (
        PARTITION BY ticker, market ORDER BY ts
        ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

{% macro bb_lower(col, n, k=2) %}
    avg({{ col }}) OVER (
        PARTITION BY ticker, market ORDER BY ts
        ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
    ) - {{ k }} * stddev_pop({{ col }}) OVER (
        PARTITION BY ticker, market ORDER BY ts
        ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}
