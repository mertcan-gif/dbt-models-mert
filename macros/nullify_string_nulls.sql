{% macro nullify_string_nulls(column_name) %}
    CASE 
        WHEN {{ column_name }} IS NULL THEN NULL
        WHEN {{ column_name }} = '' THEN NULL
        WHEN UPPER({{ column_name }}) IN ('NONE', 'NULL', 'N/A', 'NA', 'NIL', 'UNDEFINED', 'EMPTY') THEN NULL
        ELSE {{ column_name }}
    END
{% endmacro %}
