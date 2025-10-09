{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

SELECT 
  [rls_region]
  ,[rls_group]
  ,[rls_company]
  ,[rls_businessarea]
  ,[group]
  ,[company]
  ,[business_area]
  ,[businessarea_name]
  ,[date]
  ,[data_control_date]
  ,[metric]
  ,CASE
    WHEN [value] = -1 THEN NULL
    ELSE [value]
  END AS value
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM {{ ref('stg__infra_kpi_t_fact_employerprogresspayment') }}
