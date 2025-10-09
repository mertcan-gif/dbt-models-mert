{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}

SELECT
  [rls_region]
  ,[rls_group]
  ,[rls_company]
  ,[rls_businessarea]
  ,[group]
  ,[company]
  ,[current_code]
  ,[current_name]
  ,[business_area]
  ,[fiscal_year]
  ,[document_date]
  ,[material_code]
  ,[material_description]
  ,[office_name]
  ,[document_no]
  ,[document_line_item]
  ,[general_ledger_account]
  ,[category]
  ,[contract_number]
  ,[contract_eur_value]
  ,[contract_usd_value]
  ,[contract_try_value]
  ,[eur_value]
  ,[usd_value]
  ,[try_value]
FROM {{ ref('stg__rmh_kpi_t_fact_officeexpenses') }}

UNION ALL

SELECT 
  [rls_region]
  ,[rls_group]
  ,[rls_company]
  ,[rls_businessarea]
  ,[group]
  ,[company]
  ,[current_code]
  ,[current_name]
  ,[business_area]
  ,[fiscal_year]
  ,[document_date]
  ,[material_code]
  ,[material_description]
  ,[office_name]
  ,[document_no]
  ,[document_line_item]
  ,[general_ledger_account]
  ,[category]
  ,cast([contract_number] as nvarchar)
  ,[contract_eur_value]
  ,[contract_usd_value]
  ,[contract_try_value]
  ,[eur_value]
  ,[usd_value]
  ,[try_value]
FROM {{ ref('stg__rmh_kpi_t_fact_worksiteexpenses') }}