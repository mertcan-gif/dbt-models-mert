{% macro add_rls_with_custom_join(source_table_ref, rls_table_ref, join_condition) %}

SELECT 
   rls.rls_region
  ,rls.rls_group
  ,rls.rls_company
  ,rls.rls_businessarea 
  ,rls.rls_key
  ,src.*
FROM {{ source_table_ref }} src
LEFT JOIN {{ rls_table_ref }} rls ON {{ join_condition }}

{% endmacro %}