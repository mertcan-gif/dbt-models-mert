{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
WITH ddi_all AS (
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
		,ddi.[db_upload_timestamp]
		,[type]
		,[design_civil]
		,[design_architectural]
		,[design_mechanical]
		,[design_electrical]
		,[design_infrastructure_landscape]
		,[other]
		,DENSE_RANK() OVER(PARTITION BY ddi.project_id, type, reporting_date ORDER BY ddi.db_upload_timestamp DESC) AS update_rank
	FROM {{source('stg_to_kpi','raw__to_kpi_t_fact_designdrawingissue')}} ddi 
		LEFT JOIN {{source('stg_dimensions','raw__dwh_t_dim_project')}} dim ON ddi.[project_id] = dim.[project_id]   
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
		,[type] = 'total'
		,[design_type]
		,[value]
	FROM (
		SELECT 
		*
		FROM ddi_all 
		WHERE update_rank = 1
			AND [type] IN ('Total')
		) AS SourceTable

	UNPIVOT
	(
		[value] FOR [design_type] IN
		(
			design_civil,
			design_architectural,
			design_mechanical,
			design_electrical,
			design_infrastructure_landscape,
			other
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
		,[type] = 'issued'
		,[design_type]
		,[issued]

	FROM (
		SELECT 
		*
		FROM ddi_all 
		WHERE update_rank = 1
			AND [type] IN ('Issued')
		) AS SourceTable

	UNPIVOT
	(
		[issued] FOR [design_type] IN
		(
			design_civil,
			design_architectural,
			design_mechanical,
			design_electrical,
			design_infrastructure_landscape,
			other
		) 
			) AS UnpivotedTable
),

PIVOTTED_DATA AS (
	SELECT 
		*
		,[progress] = CASE WHEN COALESCE([Issued],0) = 0 THEN NULL ELSE [Issued]*1.00/[Total] END
		,[order_rank] = CASE
						WHEN design_type = 'design_civil' THEN 1
						WHEN design_type = 'design_architectural' THEN 2
						WHEN design_type = 'design_mechanical' THEN 3
						WHEN design_type = 'design_electrical' THEN 4
						WHEN design_type = 'design_infrastructure_landscape' THEN 5
						WHEN design_type = 'other' THEN 6
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
			,[design_type]
			,[value]

	  FROM UNPIVOTTED_DATA
	) ddi_pivotted
	PIVOT (
	  SUM([value])
	  FOR [type]
	  IN (
		[total],
		[issued]
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
	,[design_type] = CASE
						WHEN design_type = 'design_civil' THEN 'Civil'
						WHEN design_type = 'design_architectural' THEN 'Architectural'
						WHEN design_type = 'design_mechanical' THEN 'Mechanical'
						WHEN design_type = 'design_electrical' THEN 'Electrical'
						WHEN design_type = 'design_infrastructure_landscape' THEN 'Infrastructure/Landscape'
						WHEN design_type = 'other' THEN 'Other'
					END
	,[total]
	,[issued]
	,[progress]
	,[order_rank]
FROM PIVOTTED_DATA



