{{
  config(
    materialized = 'table',
    tags = ['scm_kpi']
  )
}}
 
select *
from {{ ref('stg__scm_kpi_t_fact_allrequestreport') }}