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
	,company = cm.RobiKisaKod
	,sug.city AS shuttle_city
	,sug.service_name AS shuttle_name
	,sug.[number_of_registered_user]
	,TRIM(CAST(vehicle_capacity AS VARCHAR)) AS vehicle_capacity
	,sug.route_km
	,sug.number_of_days_worked
	,CAST(sug.daily_amount AS money) AS daily_amount_try
	,CAST(sug.monthly_amount AS money) AS monthly_amount_try
	,CAST(sug.monthly_amount * ds.usd_value AS money) AS monthly_amount_usd
	,CAST(sug.monthly_amount * ds.eur_value AS money) AS monthly_amount_eur
	,CAST(sug.date AS date) AS date
FROM {{ source('stg_sharepoint', 'raw__rmh_kpi_t_fact_shuttleusages') }} sug
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON 'RMH' = cm.RobiKisaKod
LEFT JOIN {{ ref('dm__dimensions_t_dim_dailys4currencies') }} ds ON ds.date_string = sug.date
																		AND ds.currency = 'TRY'
	
UNION ALL

SELECT 
	[rls_region]
	,[rls_group]
	,[rls_company]
	,[rls_businessarea]
	,[company]
	,[city]
	,[shuttle_name]
	,[number_of_registered_user]
	,[vehicle_capacity]
	,[route_km]
	,[number_of_days_worked]
	,[daily_amount_try]
	,[monthly_amount_try]
	,[monthly_amount_usd]
	,[monthly_amount_eur]
	,[date]
FROM {{ ref('stg__rmh_kpi_t_fact_worksiteshuttlecosts') }}