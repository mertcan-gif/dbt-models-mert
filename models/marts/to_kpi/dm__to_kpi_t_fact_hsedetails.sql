{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
WITH hse_all AS (
	SELECT 
		dim.region AS rls_region
		,CONCAT(COALESCE(dim.[group],''),'_',COALESCE(dim.region,'')) AS rls_group
		,CONCAT(COALESCE(dim.company,''),'_',COALESCE(dim.region,'')) AS rls_company
		,CONCAT(COALESCE(dim.business_area,''),'_',COALESCE(dim.region,'')) AS rls_businessarea
		,dim.[project_id]
		,dim.company
		,dim.business_area
		,[data_entry_timestamp]
		,[reporting_date]
		,hse.[db_upload_timestamp]
		,[type]
		,[fatalities] = CAST([fatalities] AS decimal(18,2))
		,[lost_time] = CAST([lost_time] AS decimal(18,2))
		,[restricted_work_case] = CAST([restricted_work_case] AS decimal(18,2))
		,[medical_treatment_case] = CAST([medical_treatment_case] AS decimal(18,2))
		,[near_miss] = CAST([near_miss] AS decimal(18,2))
		,DENSE_RANK() OVER(PARTITION BY hse.project_id, type, reporting_date ORDER BY hse.db_upload_timestamp DESC) AS update_rank
	FROM {{source('stg_to_kpi','raw__to_kpi_t_fact_hsedetails')}} hse 
		LEFT JOIN {{source('stg_dimensions','raw__dwh_t_dim_project')}} dim ON hse.[project_id] = dim.[project_id]   
)

,UNPIVOTTED_DATA AS (
	SELECT
		[rls_region]
		,[rls_group]
		,[rls_company]
		,[rls_businessarea]
		,[project_id]
		,[company]
		,[business_area]
		,[data_entry_timestamp]
		,[reporting_date]
		,[db_upload_timestamp]
		,[type] = 'count'
		,[work_time_type]
		,[value]
	FROM (
		SELECT 
		*
		FROM hse_all 
		WHERE update_rank = 1
			AND [type] IN ('Count')
		) AS SourceTable

	UNPIVOT
	(
		[value] FOR [work_time_type] IN
		(
			fatalities,
			lost_time,
			restricted_work_case,
			medical_treatment_case,
			near_miss
		) 
			) AS UnpivotedTable


	UNION ALL


	SELECT
		[rls_region]
		,[rls_group]
		,[rls_company]
		,[rls_businessarea]
		,[project_id]
		,[company]
		,[business_area]
		,[data_entry_timestamp]
		,[reporting_date]
		,[db_upload_timestamp]
		,[type] = 'frequency'
		,[work_time_type]
		,[Frequency]
	FROM (
		SELECT 
		*
		FROM hse_all 
		WHERE update_rank = 1
			AND [type] IN ('Frequency')
		) AS SourceTable

	UNPIVOT
	(
		[Frequency] FOR [work_time_type] IN
		(
			fatalities,
			lost_time,
			restricted_work_case,
			medical_treatment_case,
			near_miss
		) 
			) AS UnpivotedTable
),

PIVOTTED_DATA AS (
	SELECT 
		* 
		,[order_rank] = CASE
							WHEN work_time_type = 'fatalities' THEN 1
							WHEN work_time_type = 'lost_time' THEN 2
							WHEN work_time_type = 'restricted_work_case' THEN 3
							WHEN work_time_type = 'medical_treatment_case' THEN 4
							WHEN work_time_type = 'near_miss' THEN 5
						END
	FROM (
		SELECT
			[rls_region]
			,[rls_group]
			,[rls_company]
			,[rls_businessarea]
			,[project_id]
			,[company]
			,[business_area]
			,[data_entry_timestamp]
			,[reporting_date]
			,[db_upload_timestamp]
			,[type]
			,[work_time_type]
			,[value]
	  FROM UNPIVOTTED_DATA
	) ddi_pivotted
	PIVOT (
	  SUM([value])
	  FOR [type]
	  IN (
		[count],
		[frequency]
	  )
	) AS PivotTable
)

SELECT
	[rls_region]
	,[rls_group]
	,[rls_company]
	,[rls_businessarea]
	,[project_id]
	,[company]
	,[business_area]
	,[data_entry_timestamp]
	,[reporting_date]
	,[db_upload_timestamp]
	,[work_time_type] = CASE
							WHEN work_time_type = 'fatalities' THEN 'Fatalities'
							WHEN work_time_type = 'lost_time' THEN 'Lost Time'
							WHEN work_time_type = 'restricted_work_case' THEN 'Restricted Work Case'
							WHEN work_time_type = 'medical_treatment_case' THEN 'Medical Treatment Case'
							WHEN work_time_type = 'near_miss' THEN 'Near Miss'
						END
	,[count]
	,[frequency]
	,[order_rank]
FROM PIVOTTED_DATA


