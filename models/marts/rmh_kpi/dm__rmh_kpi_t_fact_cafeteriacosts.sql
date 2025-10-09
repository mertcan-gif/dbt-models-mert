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
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} k ON w.bwkey = k.bwkey
)

SELECT
	rls_region = cm.RegionCode
	,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
	,rls_company = cm.RobiKisaKod + '_' + cm.RegionCode
	,rls_businessarea = '_' + cm.RegionCode
	,company = cm.RobiKisaKod
	,CAST([date] AS date) AS date
	,[office] 
	,[office_person] AS number_of_people_in_office
	,[order_count]
	,[cafeteria_usage]
	,[unit_cost] AS unit_cost_try
	,unit_cost * eur_value AS unit_cost_eur
	,unit_cost * usd_value AS unit_cost_usd
  FROM {{ source('stg_sharepoint', 'raw__rmh_kpi_t_fact_cafeteriacost') }} cc
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON 'RMH' = cm.RobiKisaKod
	LEFT JOIN {{ ref('dm__dimensions_t_dim_dailys4currencies') }} ds ON ds.date_string = CAST(cc.date AS date)
																	AND ds.currency = 'TRY'

UNION ALL

SELECT 
	rls_region = cm.RegionCode
	,rls_group = CONCAT(cm.KyribaGrup, '_', cm.RegionCode)
	,rls_company = CONCAT(cm.RobiKisaKod, '_', cm.RegionCode)
	,rls_businessarea = CONCAT(office, '_', cm.RegionCode)
	,company = cm.RobiKisaKod
	,CAST([date] AS date) AS date
	,t001w.name1 as [office]
	,[office_person]
	,[order_count]
	,[cafeteria_usage]
	,[unit_cost] AS unit_cost_try
	,unit_cost * eur_value AS unit_cost_eur
	,unit_cost * usd_value AS unit_cost_usd
FROM {{ source('stg_sharepoint', 'raw__rmh_kpi_t_fact_worksitecafeteriacost') }} wcc
	LEFT JOIN project_company_mapping pcm on pcm.werks = TRIM([office])
	LEFT JOIN {{ ref('dm__dimensions_t_dim_dailys4currencies') }} ds ON ds.date_string = CAST(wcc.date AS date)
																	AND ds.currency = 'TRY'
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON pcm.bukrs = cm.RobiKisaKod
	LEFT JOIN aws_stage.s4_odata.raw__s4hana_t_sap_t001w t001w on t001w.werks = TRIM(office)