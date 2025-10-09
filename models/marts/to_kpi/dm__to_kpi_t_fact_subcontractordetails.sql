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
		,sd.[project_id]
		,dim.company
		,dim.business_area
		,[data_entry_timestamp]
		,[reporting_date]
		,sd.[db_upload_timestamp]
		,[subcontractor]
		,[scope_of_work]
		,[contract_value_and_addendums]
		,[progress_payment_total]
		,[at_completion_contract_value]
		,[progress_payment_percentage]
		,[cpi]
		,DENSE_RANK() OVER(PARTITION BY sd.project_id, reporting_date ORDER BY sd.db_upload_timestamp DESC) AS update_rank
		,[order_rank]
	FROM {{source('stg_to_kpi','raw__to_kpi_t_fact_subcontractordetails')}} sd 
		LEFT JOIN {{source('stg_dimensions','raw__dwh_t_dim_project')}} dim ON sd.[project_id] = dim.[project_id]  
	WHERE 1=1
		AND contract_value_and_addendums IS NOT NULL
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
	,[subcontractor]
	,[scope_of_work]
	,[contract_value_and_addendums]
	,[progress_payment_total]
	,[at_completion_contract_value]
	,[progress_payment_percentage]
	,[cpi]
	,[order_rank]
FROM RAW_DATA rd
WHERE 1=1
	AND update_rank = 1

