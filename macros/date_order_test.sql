{% macro test_date_order_test(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} > {{ column_name.replace('start_date', 'end_date') }}

{% endmacro %}
