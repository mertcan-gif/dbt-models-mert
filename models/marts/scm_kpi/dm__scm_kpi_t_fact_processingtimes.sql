{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}
select 
rls_key=CONCAT(rls_businessarea, '-', rls_company, '-', rls_group),
*
from {{ ref('stg__scm_kpi_t_fact_processingtimes') }}
