{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}

SELECT 
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,CAST(creation_time AS DATE) creation_time
	,user_id as email
	,name_surname
	,report_id
	,report_name
	,report_type
	,SUM(transaction_amount) transaction_amount
FROM {{ ref('dm__metadata_kpi_t_fact_viewreportlogs') }}
WHERE 1=1
	and rls_region <> 'RUS'
GROUP BY 
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,CAST(creation_time AS DATE)
	,user_id
	,name_surname
	,report_id
	,report_name
	,report_type