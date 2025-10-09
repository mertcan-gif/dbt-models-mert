{{
  config(
    materialized = 'table',tags = ['to_kpi_draft']
    )
}}


WITH PersonelDetayStaging AS (
	SELECT
		project_id
		,data_entry_timestamp
		,db_upload_timestamp
		,reporting_date
		,[workbook_sheet]=NULL
		,reporting_group
		,reporting_sub_group
		,direct
		,indirect
		,support
	FROM
	(
		SELECT
		project_id
		,data_entry_timestamp
		,reporting_date
		,reporting_group
		,reporting_sub_group
			,db_upload_timestamp
		,[type]
		,[value]
		FROM {{source('stg_to_kpi','raw__to_kpi_t_fact_personeldetay')}}  
	  WHERE ([type] IN ('direct','indirect','support'))
	) AS SourceTable
	PIVOT(
		SUM([value])
		FOR [type] IN (
		[direct],
		[indirect],
		[support])
	) AS PivotTable


), ALL_CTE AS (
	SELECT 
		*
		,DENSE_RANK() OVER(PARTITION BY reporting_date,project_id ORDER BY db_upload_timestamp DESC) AS LAST_DB
	FROM PersonelDetayStaging
	WHERE 1=1 

)

, CTE_THIS_WEEK AS (
SELECT 
	dim.region AS rls_region
	,CONCAT(COALESCE(dim.[group],''),'_',COALESCE(dim.region,'')) AS rls_group
	,CONCAT(COALESCE(dim.company,''),'_',COALESCE(dim.region,'')) AS rls_company 
	,CONCAT(COALESCE(dim.business_area,''),'_',COALESCE(dim.region,'')) AS rls_businessarea
	,pd.[project_id]
	,pd.[data_entry_timestamp]
	,pd.[reporting_date]
	,pd.[workbook_sheet]
	,pd.[reporting_group]
	,CASE 
		WHEN [reporting_sub_group] LIKE '%Civil%' THEN 'Civil'
		ELSE [reporting_sub_group]
	END AS [reporting_sub_group]
	,CASE	
		WHEN pd.[reporting_sub_group] NOT LIKE '%Civil%' THEN NULL
		ELSE pd.[reporting_sub_group]
	END AS reporting_sub_group_detail
	,'This Week' AS chronicle_depth
	,pd.[direct]
	,pd.[indirect]
	,pd.[support]
FROM ALL_CTE pd
	LEFT JOIN {{source('stg_dimensions','raw__dwh_t_dim_project')}} dim ON dim.project_id = pd.project_id   
WHERE LAST_DB = 1
	)

, CTE_LAST_WEEK AS (
	SELECT 
		dim.region AS rls_region
		,CONCAT(COALESCE(dim.[group],''),'_',COALESCE(dim.region,'')) AS rls_group
		,CONCAT(COALESCE(dim.company,''),'_',COALESCE(dim.region,'')) AS rls_company 
		,CONCAT(COALESCE(dim.business_area,''),'_',COALESCE(dim.region,'')) AS rls_businessarea
		,pd.[project_id]
		,pd.[data_entry_timestamp]
		,DATEADD(DAY,7,pd.[reporting_date]) AS reporting_date
		,pd.[workbook_sheet]
		,pd.[reporting_group]
		,CASE 
			WHEN [reporting_sub_group] LIKE '%Civil%' THEN 'Civil'
			ELSE [reporting_sub_group]
		END AS [reporting_sub_group]
		,CASE	
			WHEN pd.[reporting_sub_group] NOT LIKE '%Civil%' THEN NULL
			ELSE pd.[reporting_sub_group]
		END AS reporting_sub_group_detail
		,'Last Week' AS chronicle_depth
		,pd.[direct]
		,pd.[indirect]
		,pd.[support]
	FROM ALL_CTE pd
		LEFT JOIN {{source('stg_dimensions','raw__dwh_t_dim_project')}} dim ON dim.project_id = pd.project_id  
	WHERE LAST_DB = 1
	)


SELECT * FROM CTE_THIS_WEEK

UNION ALL

SELECT * FROM CTE_LAST_WEEK

