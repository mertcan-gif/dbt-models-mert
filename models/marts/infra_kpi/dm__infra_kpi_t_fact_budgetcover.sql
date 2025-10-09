{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}
SELECT 
	rls_region = cm.RegionCode
	,rls_group = CONCAT(cm.KyribaGrup,'_',cm.RegionCode)
	,rls_company = CONCAT(cm.RobiKisaKod,'_',cm.RegionCode)
	,rls_businessarea = CONCAT(TRIM([Project]),'_',cm.RegionCode)
	,TRIM([Project]) AS project
	,TRIM([Bütçe Dönemi]) AS budget_period
	,TRIM([Ana Kategori]) as main_category
	,TRIM([Kategori Grubu]) as category_group
	,TRIM([Alt Kategori]) as sub_category
	,TRIM([Attribute]) as attribute
	,CAST([Value] AS money) amount
	,CAST([Data Control Date] AS DATE) AS data_control_date
 FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_budget') }} bg
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm on RobiKisaKod = 'REC'
