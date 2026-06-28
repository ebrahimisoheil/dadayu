{% macro rsi(gain_col, loss_col, n) %}
    {#
      Expects pre-computed gain_col = greatest(close - lag(close), 0)
                             loss_col = greatest(lag(close) - close, 0)
      Model CTE must compute these before calling this macro.
    #}
    100.0 - 100.0 / (
        1.0 + avg({{ gain_col }}) OVER (
            PARTITION BY ticker, market ORDER BY ts
            ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
        ) / nullif(
            avg({{ loss_col }}) OVER (
                PARTITION BY ticker, market ORDER BY ts
                ROWS BETWEEN {{ n - 1 }} PRECEDING AND CURRENT ROW
            ),
            0
        )
    )
{% endmacro %}
