{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

SELECT 
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM {{ ref('stg__infra_kpi_t_fact_isgc') }}
