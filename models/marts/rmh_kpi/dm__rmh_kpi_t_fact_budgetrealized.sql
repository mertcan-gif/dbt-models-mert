{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
SELECT 
  rls_region = cm.RegionCode
  ,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
  ,rls_company = cm.RobiKisaKod + '_' + cm.RegionCode
  ,rls_businessarea = '_' + cm.RegionCode
  ,[group] = cm.KyribaGrup
  ,company = cm.RobiKisaKod
  ,[financial_center_code]
  ,[financial_center_code_description]
  ,[commitment_item_code]
  ,[commitment_item_description]
  ,[year_month]
  ,[budget_try]
  ,[budget_usd]
  ,[budget_eur]
  ,[realized_try]
  ,[realized_usd]
  ,[realized_eur]
FROM {{ ref('stg__rmh_kpi_t_fact_rmhbudgetandrealized') }}
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON 'RMH' = cm.RobiKisaKod


