{% test range_check(model, column_name, min_value, max_value) %}
  
SELECT *
FROM {{ model }}
WHERE CAST({{ column_name }} AS DATE) < CAST('{{ min_value }}' AS DATE) 
   OR CAST({{ column_name }} AS DATE) > CAST('{{ max_value }}' AS DATE)

{% endtest %}