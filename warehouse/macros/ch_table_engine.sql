{% macro ch_table_engine(order_by, partition_by=none, version_col='ingested_at') %}
    ENGINE = ReplacingMergeTree({{ version_col }})
    ORDER BY ({{ order_by }})
    {% if partition_by %}
    PARTITION BY {{ partition_by }}
    {% endif %}
{% endmacro %}
