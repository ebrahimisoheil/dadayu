{% macro time_bucket(col, interval) %}
    toStartOfInterval({{ col }}, INTERVAL {{ interval }})
{% endmacro %}
