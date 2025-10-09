{{
  config(
    materialized = 'view',tags = ['to_kpi']
    )
}}

SELECT 
	dim.region AS rls_region
	,CONCAT(COALESCE(dim.[group],''),'_',COALESCE(dim.region,'')) AS rls_group
    ,CONCAT(COALESCE(dim.company,''),'_',COALESCE(dim.region,'')) AS rls_company
    ,CONCAT(COALESCE(dim.business_area,''),'_',COALESCE(dim.region,'')) AS rls_businessarea
	,raw_data.*
FROM (
	SELECT 
		project_id
		,physical_progress
		,'physical_progress' as type
		,[timestamp] = reporting_date
	FROM {{ ref('dm__to_kpi_t_dim_all') }}

	UNION ALL

	SELECT 
		project_id
		,total_personnel
		,'total_employee' as type
		,[timestamp] = reporting_date
	FROM {{ ref('dm__to_kpi_t_dim_all') }}
) raw_data
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} dim ON raw_data.project_id = dim.project_id