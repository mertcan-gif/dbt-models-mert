{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

WITH _raw AS (
SELECT
	rls_region = cmp.RegionCode
	,rls_group = CONCAT(cmp.KyribaGrup, '_',cmp.RegionCode)
	,rls_company = CONCAT(TRIM([company]),'_',cmp.RegionCode)
	,rls_businessarea = CONCAT(TRIM([project_code]), '_', cmp.RegionCode)
	,TRIM([company]) as company
	,TRIM([project_code]) as [project_code]
	,TRIM([project_name]) as [project_short_name]
	,t001w.name1 AS project_name
	,[row_number]
	,TRIM([category]) as [category]
	,TRIM([sub category]) as [sub category]
	,CAST([amount] AS money) AS amount
	,CAST([date] AS date) [date]
	,CAST(data_control_date AS date) AS data_control_date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_commitment') }} cm
  LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cmp on TRIM(cm.company) = cmp.KyribaKisaKod
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w on t001w.werks = TRIM(cm.project_code)

)
SELECT 
  *
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
