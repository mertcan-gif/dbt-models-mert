
{{
  config(
    materialized = 'table',tags = ['nwc_kpi_draft','stockaging_draft']
    )
}}

SELECT 
  rls_region
  ,rls_group
  ,rls_company
  ,rls_businessarea
  ,company = RBUKRS
  ,businessarea = RBUSA
  ,material_code = MATNR
  ,beginning_period_amount = a
  ,current_amount = b
  ,cost = c
  ,turnover = TURNOVER
FROM {{ ref('stg__nwc_kpi_t_fact_stokdevir') }}
