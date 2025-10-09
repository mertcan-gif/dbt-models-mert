{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
WITH project_company_mapping AS (
SELECT
	name1
	,WERKS
	,w.BWKEY
	,bukrs
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} w
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }}   k ON w.bwkey = k.bwkey
)

,worksite_usage AS ( 
SELECT 
	rls_region = cm.RegionCode
	,rls_group = CONCAT(cm.KyribaGrup, '_', cm.RegionCode)
	,rls_company = CONCAT(cm.RobiKisaKod, '_', cm.RegionCode)
	,rls_businessarea = CONCAT(pcm.werks, '_', cm.RegionCode)
	,company = cm.RobiKisaKod
	,[city]
	,[service_name]
	,[number_of_registered_user]
	,[vehicle_capacity]
	,[route_km]
	,[number_of_days_worked]
	,CASE
		WHEN daily_amount IS NULL THEN CAST(monthly_amount as money) / CAST(number_of_days_worked as int)
		ELSE daily_amount
	END AS daily_amount_try
	,CASE
		WHEN monthly_amount IS NULL THEN CAST(daily_amount as money) * CAST(number_of_days_worked as int)
		ELSE monthly_amount
	END AS monthly_amount_try
	,eur_value
	,usd_value
	,CAST([date] AS date) date
FROM {{ source('stg_sharepoint', 'raw__rmh_kpi_t_fact_worksiteshuttleusages') }} wsu
	LEFT JOIN project_company_mapping pcm on pcm.werks = TRIM([worksite])
	LEFT JOIN {{ ref('dm__dimensions_t_dim_dailys4currencies') }} ds ON ds.date_string = CAST(wsu.date AS date)
																	                                AND ds.currency = 'TRY'
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON pcm.bukrs = cm.RobiKisaKod
WHERE collar_type IS NOT NULL
	AND city <> 'DUMMY'
	)

SELECT 
	[rls_region]
	,[rls_group]
	,[rls_company]
	,[rls_businessarea]
	,[company]
	,[city]
	,[service_name] as shuttle_name
	,[number_of_registered_user]
	,[vehicle_capacity]
	,[route_km]
	,[number_of_days_worked]
	,[daily_amount_try]
	,[monthly_amount_try]
	,CAST(monthly_amount_try * usd_value AS money) AS monthly_amount_usd
	,CAST(monthly_amount_try * eur_value AS money) AS monthly_amount_eur
	,[date]
FROM worksite_usage