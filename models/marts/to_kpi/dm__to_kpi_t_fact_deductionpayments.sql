{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
SELECT 
  [rls_region]
  ,[rls_group]
  ,[rls_company]
  ,[rls_businessarea]
  ,[company]
  ,[businessarea]
  ,[businessarea_name]
  ,[deduction_id]
  ,[deduction_name]
  ,[transaction_quantity]
  ,[unit_cost]
  ,[posting_date]
  ,[currency]
FROM {{ ref('stg__to_kpi_t_fact_progresspaymentdeduction') }}