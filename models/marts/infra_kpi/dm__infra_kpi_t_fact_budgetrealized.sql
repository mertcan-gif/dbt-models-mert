{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

SELECT 
	rls_region = cm.RegionCode
	,rls_group = CONCAT(cm.KyribaGrup,'_',cm.RegionCode)
	,rls_company = CONCAT(cm.RobiKisaKod,'_',cm.RegionCode)
	,rls_businessarea = CONCAT(TRIM(Proje),'_',cm.RegionCode)
	,TRIM([Proje]) as project
	,[Bütçe Dönemi] as budget_period
	,CAST([Date] AS date) as [date]
	,TRIM([Kategori]) AS category
	,CAST([Amount Eur] AS float) amount_eur
	,CAST([Data Control Date] AS DATE) AS data_control_date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_budgetrealized') }} br
    LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm on cm.RobiKisaKod = 'REC'

