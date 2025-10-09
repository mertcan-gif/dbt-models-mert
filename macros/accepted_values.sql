{% test accepted_values(model, column_name, values_list) %}

SELECT *
FROM {{ model }}
WHERE UPPER(LTRIM(RTRIM({{ column_name }}))) COLLATE Latin1_General_CI_AI 
      NOT IN ( {% for value in values_list %} UPPER(LTRIM(RTRIM('{{ value }}'))) COLLATE Latin1_General_CI_AI {% if not loop.last %}, {% endif %} {% endfor %} )

{% endtest %}