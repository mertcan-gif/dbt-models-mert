{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
WITH qa_qc_all AS (
	SELECT 
		dim.region AS rls_region
		,CONCAT(COALESCE(dim.[group],''),'_',COALESCE(dim.region,'')) AS rls_group
		,CONCAT(COALESCE(dim.company,''),'_',COALESCE(dim.region,'')) AS rls_company
		,CONCAT(COALESCE(dim.business_area,''),'_',COALESCE(dim.region,'')) AS rls_businessarea
		,qaqc.[project_id]
		,dim.company
		,dim.business_area
		,[data_entry_timestamp]
		,[reporting_date]
		,qaqc.[db_upload_timestamp]
		,[open_or_close]
		,[qa_qc_civil] = IIF([qa_qc_civil] IS NOT NULL, CAST([qa_qc_civil] AS float), 0)
		,[qa_qc_architectural] = IIF([qa_qc_architectural] IS NOT NULL, CAST([qa_qc_architectural] AS float), 0)
		,[qa_qc_mechanical] = IIF([qa_qc_mechanical] IS NOT NULL, CAST([qa_qc_mechanical] AS float), 0)
		,[qa_qc_electrical] = IIF([qa_qc_electrical] IS NOT NULL, CAST([qa_qc_electrical] AS float), 0)
		,[qa_qc_infrastructure] = IIF([qa_qc_infrastructure] IS NOT NULL, CAST([qa_qc_infrastructure] AS float), 0)
		,[qa_qc_other] = IIF([qa_qc_other] IS NOT NULL, CAST([qa_qc_other] AS float), 0)
  		,DENSE_RANK() OVER(PARTITION BY qaqc.project_id, open_or_close, reporting_date ORDER BY qaqc.db_upload_timestamp DESC) AS update_rank
	FROM {{source('stg_to_kpi','raw__to_kpi_t_fact_qaqcncr')}} qaqc    
			LEFT JOIN {{source('stg_dimensions','raw__dwh_t_dim_project')}} dim ON qaqc.[project_id] = dim.[project_id]   
	WHERE open_or_close IS NOT NULL
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
		,[open_or_close] = 'open'
		,[qa_qc_type]
		,[value]
	FROM (
		SELECT 
		*
		FROM qa_qc_all 
		WHERE update_rank = 1
			AND [open_or_close] IN ('Açik')
		) AS SourceTable

	UNPIVOT
	(
		[value] FOR [qa_qc_type] IN
		(
		[qa_qc_civil]
		,[qa_qc_architectural]
		,[qa_qc_mechanical]
		,[qa_qc_electrical]
		,[qa_qc_infrastructure]
		,[qa_qc_other]
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
		,[open_or_close] = 'closed'
		,[qa_qc_type]
		,[value]
	FROM (
		SELECT 
		*
		FROM qa_qc_all 
		WHERE update_rank = 1
			AND [open_or_close] IN ('Kapali')
		) AS SourceTable

	UNPIVOT
	(
		[value] FOR [qa_qc_type] IN
		(
		[qa_qc_civil]
		,[qa_qc_architectural]
		,[qa_qc_mechanical]
		,[qa_qc_electrical]
		,[qa_qc_infrastructure]
		,[qa_qc_other]
		) 
			) AS UnpivotedTable
),

RAW_PIVOT AS (
	SELECT * FROM (
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
			,[open_or_close]
			,[qa_qc_type]
			,[value]
	  FROM UNPIVOTTED_DATA
	) ddi_pivotted
	PIVOT (
	  SUM([value])
	  FOR [open_or_close]
	  IN (
		[Open],
		[Closed]
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
	,[qa_qc_type] = CASE
						WHEN qa_qc_type = 'qa_qc_civil' THEN 'Civil'
						WHEN qa_qc_type = 'qa_qc_architectural' THEN 'Architectural'
						WHEN qa_qc_type = 'qa_qc_mechanical' THEN 'Mechanical'
						WHEN qa_qc_type = 'qa_qc_electrical' THEN 'Electrical'
						WHEN qa_qc_type = 'qa_qc_infrastructure' THEN 'Infrastructure'
						WHEN qa_qc_type = 'qa_qc_other' THEN 'Other'
					END
						
	,[open] = COALESCE([Open],0)
	,[closed] = COALESCE([Closed],0)
	,[total] = COALESCE([Open],0) + COALESCE([Closed],0)
	,[case_closed] =  CASE 
							WHEN COALESCE([Open],0) + COALESCE([Closed],0) = 0 THEN NULL 
							ELSE COALESCE([Closed],0) / (COALESCE([Open],0) + COALESCE([Closed],0)) END
	,[overall_ratio] = CASE 
							WHEN (COALESCE([Open],0) + COALESCE([Closed],0)) = 0 THEN NULL 
							ELSE (COALESCE([Open],0) + COALESCE([Closed],0))
								 /
								 (SELECT SUM(COALESCE(RP2.[Open],0) + COALESCE(RP2.[Closed],0)) FROM RAW_PIVOT RP2 
																							WHERE RP2.project_id = RP.project_id
																							AND  RP2.reporting_date = RP.reporting_date
																							AND  RP2.data_entry_timestamp = RP.data_entry_timestamp)
						END

	,[order_rank] = CASE
						WHEN qa_qc_type = 'qa_qc_civil' THEN 1
						WHEN qa_qc_type = 'qa_qc_architectural' THEN 2
						WHEN qa_qc_type = 'qa_qc_mechanical' THEN 3
						WHEN qa_qc_type = 'qa_qc_electrical' THEN 4
						WHEN qa_qc_type = 'qa_qc_infrastructure' THEN 5
						WHEN qa_qc_type = 'qa_qc_other' THEN 6
					END

FROM RAW_PIVOT RP


