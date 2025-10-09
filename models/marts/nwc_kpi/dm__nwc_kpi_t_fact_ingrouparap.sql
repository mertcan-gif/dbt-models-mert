{{
  config(
    materialized = 'table',tags = ['ingroup_arap_hourly']
    )
}}

SELECT
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(ia.company  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(ia.business_area  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,ia.*
	,kyriba_group = kuc.KyribaGrup
	,customer_vendor_group = kuc2.KyribaGrup
	,fin_group = fac.[group]
	,customer_vendor_fin_group = fac2.[group]
	,CASE WHEN due_days<0 THEN 'Overdue' ELSE 'Outstanding' END AS category
	,CASE
		WHEN ABS(due_days) <=30 THEN '0-30 Days'
		WHEN ABS(due_days) <=60 THEN '31-60 Days'
		WHEN ABS(due_days) <=90 THEN '61-90 Days'
		WHEN ABS(due_days) <=180 THEN '91-180 Days'
		WHEN ABS(due_days) <=365 THEN '181-365 Days'
		ELSE '>365 Days'
	END as due_category
	,main_account_mapping = CASE WHEN faa.main_account_type IS NULL THEN N'Tanımsız' ELSE faa.main_account_type END
FROM {{ ref('stg__nwc_kpi_t_fact_ingrouparap') }} ia
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON ia.company = kuc.RobiKisaKod 
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc2 ON ia.customer_vendor_code = kuc2.RobiKisaKod 
	LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_financialaffairscompanies') }} fac ON ia.company = fac.company 
	LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_financialaffairscompanies') }} fac2 ON ia.customer_vendor_code = fac2.company 
	LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_financialaffairsaccounts') }} faa ON ia.general_ledger_account = faa.main_account