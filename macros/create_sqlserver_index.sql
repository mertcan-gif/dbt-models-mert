{% macro create_sqlserver_index(table_name, column_names, unique=false) %}

  {%- set index_name = "ix_" ~ table_name ~ "_" ~ column_names | join("_") -%}
  {%- set columns = column_names | join(", ") -%}

  CREATE {% if unique %}UNIQUE{% endif %} INDEX {{ index_name }} ON {{ table_name }} ({{ columns }});

{% endmacro %}
