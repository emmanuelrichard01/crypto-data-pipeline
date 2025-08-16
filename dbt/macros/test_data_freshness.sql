{% macro test_data_freshness(model, column, max_hours=2) %}
    SELECT COUNT(*)
    FROM {{ model }}
    WHERE {{ column }} < (CURRENT_TIMESTAMP - INTERVAL '{{ max_hours }} hours')
{% endmacro %}
