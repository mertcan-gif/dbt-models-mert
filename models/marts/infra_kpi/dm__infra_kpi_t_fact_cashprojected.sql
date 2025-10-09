{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

SELECT 
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea = CONCAT(TRIM([Proje]), '_', rls_region)
	,company = 'REC'
	,CAST([Date] AS DATE) AS date
	,TRIM([Proje]) AS project
	,t001w.name1 AS project_name
	,CASE	
		WHEN TRIM([Proje]) = 'R003' THEN 'KMO'
		WHEN TRIM([Proje]) = 'R004' THEN 'MAG'
		ELSE ''
	END AS project_shortname
	,CAST([Nakit Durumu] AS money) cash_amount
	,TRIM([Forecast Durumu]) forecast_status
	,CAST(data_control_date AS date) AS data_control_date
	,label_select
FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_cashprojected') }} cp
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w on TRIM(cp.Proje) = t001w.werks
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on cm.RobiKisaKod = 'REC'
