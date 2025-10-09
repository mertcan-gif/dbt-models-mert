{{
  config(
    materialized = 'table',tags = ['to_kpi_draft']
    )
}}

WITH TR_TO_EN AS (
	SELECT
		reporting_group_category
		,english_terminology
		,turkish_terminology = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(turkish_terminology COLLATE database_default),N'İ',N'I'),N'Ğ',N'G'),N'Ş',N'S'),N'Ö',N'O'),N'Ç',N'C'),N'Ü',N'U')
	FROM
		{{ source('stg_to_kpi', 'raw__to_kpi_t_dim_ilerlemedetayterminologytrtoen') }}
),

ID_GROUPS_FORMATTED AS (
		SELECT [project_id]
			  ,[data_entry_timestamp]
			  ,[reporting_date]
			  ,[reporting_group] =  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER([reporting_group] COLLATE database_default),N'İ',N'I'),N'Ğ',N'G'),N'Ş',N'S'),N'Ö',N'O'),N'Ç',N'C'),N'Ü',N'U')
			  ,[reporting_sub_group] =  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER([reporting_sub_group] COLLATE database_default),N'İ',N'I'),N'Ğ',N'G'),N'Ş',N'S'),N'Ö',N'O'),N'Ç',N'C'),N'Ü',N'U')
			  ,[earned_man_hour]
			  ,[actual_man_hour]
			  ,[planned_man_hour]
			  ,[total_man_hour]
			  ,[total_qty]
			  ,[realized_qty]
			  ,[qty_progress]
			  ,[db_upload_timestamp]
			  ,order_rank
		  FROM {{ source('stg_to_kpi', 'raw__to_kpi_t_fact_ilerlemedetay') }}
)

	SELECT 
		dim.region AS rls_region
		,CONCAT(COALESCE(dim.[group],''),'_',COALESCE(dim.region,'')) AS rls_group
		,CONCAT(COALESCE(dim.company,''),'_',COALESCE(dim.region,'')) AS rls_company
		,CONCAT(COALESCE(dim.business_area,''),'_',COALESCE(dim.region,'')) AS rls_businessarea
		,id.[project_id]
		,id.[data_entry_timestamp]
		,id.[reporting_date]
		,[workbook_sheet] = NULL
		,id.[reporting_group]
		,CASE
			WHEN id.[reporting_sub_group] <> 'N/A' THEN id.[reporting_sub_group] 
			ELSE id.[reporting_group]
		END AS [reporting_sub_group]
		,id.[earned_man_hour]
		,id.[actual_man_hour]
		,id.[planned_man_hour]
		,id.[total_man_hour]
		,[weekly_physical_progress] = NULL
		,[weekly_cpi] = NULL
		,[weekly_spi] = NULL
		,[cumulative_physical_progress] = NULL
		,[cumulative_cpi] = NULL
		,[cumulative_spi] = NULL
		,total_qty
		,realized_qty
		,qty_progress
		,order_rank
	FROM (
		SELECT [project_id]
			  ,[data_entry_timestamp]
			  ,[reporting_date]
			  ,[reporting_group] = 
					(SELECT TOP 1 [english_terminology] 
					 FROM TR_TO_EN
					 WHERE turkish_terminology = ID_GROUPS_FORMATTED.reporting_group)
			  ,[reporting_sub_group] = 
	  					(SELECT TOP 1 [english_terminology] 
					 FROM TR_TO_EN
					 WHERE turkish_terminology = ID_GROUPS_FORMATTED.[reporting_sub_group])
			  ,[earned_man_hour]
			  ,[actual_man_hour]
			  ,[planned_man_hour]
			  ,[total_man_hour]
			  ,[total_qty]
			  ,[realized_qty]
			  ,[qty_progress]
			  ,[db_upload_timestamp]
			  ,order_rank
			  ,DENSE_RANK() OVER(PARTITION BY reporting_date,project_id ORDER BY db_upload_timestamp DESC) LAST_DB
		  FROM ID_GROUPS_FORMATTED 
		  WHERE 1=1 
			AND (earned_man_hour <> 0 OR actual_man_hour <> 0 OR planned_man_hour <> 0 OR total_man_hour <> 0)
		
			) id
		LEFT JOIN {{source('stg_dimensions','raw__dwh_t_dim_project')}} dim ON dim.project_id = id.project_id  
		WHERE LAST_DB = 1



