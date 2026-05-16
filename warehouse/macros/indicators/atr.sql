{% macro atr(high, low, close, n) %}
    avg(
        greatest(
            {{ high }} - {{ low }},
            abs({{ high }} - lagInFrame({{ close }}, 1, {{ close }}) OVER (PARTITION BY ticker, market ORDER BY ts)),
            abs({{ low }}  - lagInFrame({{ close }}, 1, {{ close }}) OVER (PARTITION BY ticker, market ORDER BY ts))
        )
    ) OVER (
        PARTITION BY ticker, market ORDER BY ts
        ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}
