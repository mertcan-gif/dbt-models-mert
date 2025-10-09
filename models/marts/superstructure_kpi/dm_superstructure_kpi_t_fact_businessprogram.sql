{{
  config(
    materialized = 'table',tags = ['superstructure_kpi']
    )
}}	

WITH business_program_cte AS (

	--SELECT * FROM aws_stage.sharepoint.raw__superstructure_kpi_t_fact_businessprogrambu1 -- eksik

	--UNION ALL

	SELECT 
		[date]
		,[company]	
		,[business_area]	
		,[type]	
		,[realized]	
		,[target]
		,[not]
		,[db_upload_timestamp]
 	FROM {{ source('stg_sharepoint', 'raw__superstructure_kpi_t_fact_businessprogrambu3') }}

	UNION ALL

	SELECT 
		[date]
		,[company]	
		,[business_area]	
		,[type]	
		,[realized]	
		,[target]
		,[not]
		,[db_upload_timestamp]
 	FROM {{ source('stg_sharepoint', 'raw__superstructure_kpi_t_fact_businessprogrambu4') }}

	UNION ALL

	SELECT 
		[date]
		,[company]	
		,[business_area]	
		,[type]	
		,[realized]	
		,[target]
		,[not]
		,[db_upload_timestamp]
 	FROM {{ source('stg_sharepoint', 'raw__superstructure_kpi_t_fact_businessprogramr010') }}

	UNION ALL

	SELECT 
		[date]
		,[company]	
		,[business_area]	
		,[type]	
		,[realized]	
		,[target]
		,[not]
		,[db_upload_timestamp]
 	FROM {{ source('stg_sharepoint', 'raw__superstructure_kpi_t_fact_businessprogramr054') }}

	UNION ALL

	SELECT 
		[date]
		,[company]	
		,[business_area]	
		,[type]	
		,[realized]	
		,[target]
		,[not]
		,[db_upload_timestamp]
 	FROM {{ source('stg_sharepoint', 'raw__superstructure_kpi_t_fact_businessprogramr055') }}

)

SELECT
	[rls_region] = cm.RegionCode
	,[rls_group] = CONCAT(cm.[KyribaGrup], '_', cm.RegionCode)
	,[rls_company] = CONCAT(bp.company, '_', cm.RegionCode)
	,[rls_businessarea] = CONCAT(bp.business_area, '_', cm.RegionCode)
	,bp.*
	,date_normalized = CAST(EOMONTH([date]) AS DATE)
FROM business_program_cte bp
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON cm.RobiKisaKod = bp.company