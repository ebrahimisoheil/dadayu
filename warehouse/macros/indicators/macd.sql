{% macro macd_line(ema_fast_col, ema_slow_col) %}
    {{ ema_fast_col }} - {{ ema_slow_col }}
{% endmacro %}

{% macro macd_signal(ema_fast_col, ema_slow_col, signal_n) %}
    {# Signal = EMA of MACD line; computed via arrayFold over the window #}
    arrayFold(
        (acc, x) -> acc + (2.0 / ({{ signal_n }} + 1)) * (x - acc),
        groupArray({{ ema_fast_col }} - {{ ema_slow_col }}) OVER (
            PARTITION BY ticker, market ORDER BY ts
            ROWS BETWEEN {{ signal_n - 1 }} PRECEDING AND CURRENT ROW
        ),
        toFloat64(0)
    )
{% endmacro %}

{% macro macd_hist(ema_fast_col, ema_slow_col, signal_n) %}
    ({{ ema_fast_col }} - {{ ema_slow_col }}) - {{ macd_signal(ema_fast_col, ema_slow_col, signal_n) }}
{% endmacro %}
