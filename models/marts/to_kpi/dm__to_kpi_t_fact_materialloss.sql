{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
WITH RAW_DATA AS (

	SELECT 
		dim.region AS rls_region
		,CONCAT(COALESCE(dim.[group],''),'_',COALESCE(dim.region,'')) AS rls_group
		,CONCAT(COALESCE(dim.company,''),'_',COALESCE(dim.region,'')) AS rls_company
		,CONCAT(COALESCE(dim.business_area,''),'_',COALESCE(dim.region,'')) AS rls_businessarea
		,mz.[project_id]
		,dim.company
		,dim.business_area
		,[data_entry_timestamp]
		,[reporting_date]
		,mz.[db_upload_timestamp]
		,[waste_material_name] -- İlerleme Detay'daki gibi bir translation dimension'u oluşturulacak
		,[waste_percentage]
		,[realized_waste_percentage]
		,[order_rank]
		,DENSE_RANK() OVER(PARTITION BY mz.project_id, reporting_date ORDER BY mz.db_upload_timestamp DESC) AS update_rank
	FROM {{source('stg_to_kpi','raw__to_kpi_t_fact_materialloss')}} mz    
		LEFT JOIN {{source('stg_dimensions','raw__dwh_t_dim_project')}} dim ON mz.[project_id] = dim.[project_id]   
)

SELECT
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,[project_id]
	,company
	,business_area
	,[data_entry_timestamp]
	,[reporting_date]
	,[db_upload_timestamp]
	,[waste_material_name]
	,[waste_percentage]
	,[realized_waste_percentage]
	,[order_rank]
FROM RAW_DATA
WHERE update_rank = 1


