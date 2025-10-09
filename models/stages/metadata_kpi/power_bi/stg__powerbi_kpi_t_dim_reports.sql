{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}


/************ */
WITH last_reports as (
    select *
    from (
        SELECT
            *
            ,ROW_NUMBER() OVER(Partition by id order by reporting_date DESC) report_index
        FROM   {{ source('stg_powerbi_kpi', 'raw__powerbi_t_dim_reports') }} rpr
        ) cte
    where report_index = 1
    ),
last_workspaces as (
    SELECT
        *
    FROM {{ source('stg_powerbi_kpi', 'raw__powerbi_t_dim_workspaces') }}  wsp
    WHERE wsp.db_upload_timestamp = 
        (SELECT MAX(wsp2.db_upload_timestamp) FROM {{ source('stg_powerbi_kpi', 'raw__powerbi_t_dim_workspaces') }} wsp2 )
    )
,all_reports AS (
	SELECT 
			[report_id],
			[report_name],
			[report_type],
			[workspace_id],
			creation_time,
			ROW_NUMBER() OVER(PARTITION BY [report_id] ORDER BY CAST(creation_time AS DATETIME) DESC) DUPS
	FROM {{ source('stg_powerbi_kpi', 'raw__powerbi_t_fact_viewreportlogs') }} 
	WHERE report_id <> 'nan'
), all_reports_removed_dups as (
		SELECT *
		FROM ALL_REPORTS
		WHERE DUPS = 1
	)

select 
	rp.[report_id] as [id]
    ,rp.[report_type]
    ,rp.[report_name] as [name]
    ,[created_date_time]
    ,[modified_date_time]
    ,COALESCE([modified_by],'deleted') as [modified_by]
    ,COALESCE([created_by],'deleted') as [created_by]
    ,rp.[workspace_id]
    ,coalesce(lw.[name],'deleted') as [sub_segment]
    ,'Power BI' as [segment]
from all_reports_removed_dups rp
	left join last_reports  lr ON rp.report_id = lr.id
	left join last_workspaces lw ON rp.workspace_id = lw.id