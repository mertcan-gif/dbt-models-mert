{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
WITH ALL_CTE AS (
	SELECT 
		dim.region
		,dim.[group]
		,dim.company
		,dim.business_area 
		,sc.[project_id]
		,sc.[data_entry_timestamp] 
		,sc.[reporting_date] 
		,[workbook_sheet] = NULL
		,sc.[chronicle_depth] 
		,sc.[cumulative_early_planned]
		,sc.[baseline_before_revision_r5] 
		,sc.[baseline_before_revision_r4]
		,sc.[baseline_before_revision_r3]
		,sc.[baseline_before_revision_r2]
		,sc.[baseline_before_revision_r1]
		,sc.[cumulative_realized]
		,sc.[cumulative_late_planned]
		,DENSE_RANK() OVER(PARTITION BY sc.project_id,sc.reporting_date ORDER BY sc.db_upload_timestamp DESC) AS LAST_DB
	FROM {{source('stg_to_kpi','raw__to_kpi_t_fact_scurve')}} sc   
		LEFT JOIN {{source('stg_dimensions','raw__dwh_t_dim_project')}} dim ON dim.project_id = sc.project_id   
),

UP_TO_DATE_DATA AS (
	SELECT 
		region
		,[group]
		,company
		,business_area
		,[project_id]
		,[data_entry_timestamp] 
		,[reporting_date] 
		,[workbook_sheet] = NULL
		,[chronicle_depth] 
		,[cumulative_early_planned]
		,[baseline_before_revision_r5] 
		,[baseline_before_revision_r4]
		,[baseline_before_revision_r3]
		,[baseline_before_revision_r2]
		,[baseline_before_revision_r1]
		,[cumulative_realized]
		,[cumulative_late_planned]
	FROM ALL_CTE
	WHERE LAST_DB = 1
)

SELECT 
	[rls_region] = region
	,[rls_group] = CONCAT(COALESCE([group],''),'_',COALESCE([region],''))
	,[rls_company] = CONCAT(COALESCE([company],''),'_',COALESCE([region],''))
	,[rls_businessarea] = CONCAT(COALESCE([business_area],''),'_',COALESCE([region],''))
	,[project_id]
	,[company]
	,[business_area]
	,[data_entry_timestamp]
	,[reporting_date]
	,[workbook_sheet]
	,[chronicle_depth]
	,[cumulative_early_planned] =
		CASE 
			WHEN [chronicle_depth] >= 
				(
				select 
					DATEADD(DAY,7,max([chronicle_depth]))
				from {{source('stg_to_kpi','raw__to_kpi_t_fact_scurve')}} t2
				where t2.project_id = t1.project_id and  t2.reporting_date = t1.reporting_date AND cumulative_realized IS NOT NULL
					)
			AND FORMAT([chronicle_depth],'MM-yyy') = FORMAT(reporting_date,'MM-yyy')
					
				THEN NULL
			ELSE [cumulative_early_planned]
		END
	,[baseline_before_revision_r5]
	,[baseline_before_revision_r4]
	,[baseline_before_revision_r3]
	,[baseline_before_revision_r2]
	,[baseline_before_revision_r1]
	,[cumulative_realized]
	,[cumulative_late_planned]
FROM UP_TO_DATE_DATA t1

