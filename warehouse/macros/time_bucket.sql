{% macro time_bucket(col, interval) %}
    date_bin(INTERVAL '{{ interval }}', {{ col }}, TIMESTAMP '1970-01-01')
{% endmacro %}
