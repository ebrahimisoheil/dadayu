{% macro atr(true_range_col, n) %}
    {#
      Expects pre-computed true_range_col =
        greatest(high - low, abs(high - prev_close), abs(low - prev_close))
      Model CTE must compute prev_close before calling this macro.
    #}
    avg({{ true_range_col }}) OVER (
        PARTITION BY ticker, market ORDER BY ts
        ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}
