{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
SELECT
	dim.region AS rls_region
	,CONCAT(COALESCE(dim.[group],''),'_',COALESCE(dim.region,'')) AS rls_group
	,CONCAT(COALESCE(dim.company,''),'_',COALESCE(dim.region,'')) AS rls_company 
	,CONCAT(COALESCE(dim.business_area,''),'_',COALESCE(dim.region,'')) AS rls_businessarea
	,dim.project_id
	,cd.[business_area]
	,[type] = [project_status]
	,cd.[status]
	,[project]
	,[total] = COUNT(*)
	,[subcontractor_claim_try] = SUM(COALESCE([claim_total_amount_try],0))
	,[construction_site_expenses_try] = SUM(COALESCE([construction_site_total_amount_try],0))
	,[head_office_expenses_try] = SUM(COALESCE([head_office_total_amount_try],0))
	,[confirmed_amount_try] = SUM(COALESCE([approved_total_amount_try],0))
	,[difference_try] = SUM(COALESCE([claim_total_amount_try],0)) - SUM(COALESCE([approved_total_amount_try],0))
FROM {{source('stg_to_kpi','raw__to_kpi_t_fact_claimdata')}} cd  
	LEFT JOIN {{source('stg_to_kpi','raw__to_kpi_t_dim_consolidateddata')}} con ON con.sap_business_area = cd.business_area  
	LEFT JOIN {{source('stg_dimensions','raw__dwh_t_dim_project')}} dim ON dim.business_area = cd.business_area  
WHERE 1=1
	AND cd.business_area IS NOT NULL
GROUP BY 
	dim.region
	,dim.[group]
	,dim.company
	,dim.business_area
	,dim.project_id
	,cd.[business_area]
	,[project_status]
	,cd.[status]
	,[project]
	


