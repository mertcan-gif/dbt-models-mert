{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

WITH _raw AS (
SELECT
  rls_region = cm.RegionCode
  ,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
  ,rls_company = cm.KyribaKisaKod + '_' + cm.RegionCode
  ,rls_businessarea = TRIM(business_area) + '_' + cm.RegionCode
  ,cm.KyribaGrup as [group]
  ,[company]
  ,[business_area]
  ,[businessarea_name]
  ,[budget_period]
  ,[realized_or_planned]
  ,CAST([cash_income] AS money) [cash_income]
  ,CAST([cash_expense] AS money) [cash_expense] 
  ,[currency]
  ,CAST([cumulative_amount_try] AS money) [cumulative_amount_try]
  ,CAST([cumulative_amount_eur] AS money) [cumulative_amount_eur]
  ,CAST([date] AS date) date
FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_cashflow') }} cf
  LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on TRIM(cf.company) = cm.KyribaKisaKod
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t ON TRIM(cf.business_area) = t.werks
)

SELECT	
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
