{% set violations = [] %}

{% for node in graph.nodes.values() %}
    {% if node.resource_type == 'model'
        and node.package_name == 'dadayu_warehouse'
        and node.path.startswith('03_marts/briefing/') %}
        {% for dep_id in node.depends_on.nodes %}
            {% set dep_node = graph.nodes.get(dep_id) %}
            {% if dep_node is not none and dep_node.name.startswith('int_') %}
                {% do violations.append(node.name ~ ' -> ' ~ dep_node.name) %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endfor %}

{% if violations | length > 0 %}
    SELECT '{{ violations | join(", ") }}' AS violation
{% else %}
    SELECT 1 AS ok WHERE false
{% endif %}
