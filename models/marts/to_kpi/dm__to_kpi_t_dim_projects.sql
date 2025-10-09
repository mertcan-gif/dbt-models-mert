{{
  config(
    materialized = 'table',tags = ['nwc_kpi','dimensions','project_dimension']
    )
}}


SELECT
	[dim_project].[region] AS rls_region
	,CONCAT(COALESCE([dim_project].[group],''),'_',COALESCE([dim_project].[region],'')) AS rls_group
	,CONCAT(COALESCE([dim_project].[company],''),'_',COALESCE([dim_project].[region],'')) AS rls_company
	,CONCAT(COALESCE([dim_project].[business_area],''),'_',COALESCE([dim_project].[region],'')) AS rls_businessarea
    ,[reporting_date]
	,[dim_project].[project_id]
	,[dim_project].[business_area]
	,[dim_project].[risk_portal_project_id]
	,[dim_project].[res_project_id]
	,CAST([dim_project].[rsafe_id] AS VARCHAR) AS [rsafe_id]
    ,[dim_project].[group]
    ,[dim_project].[company]
    ,[dim_project].[project_name] AS [name]
	,[dim_project].[project_shortname] AS [project_shortname]
    ,[dim_project].[latitude] 
    ,[dim_project].[longitude] 
    ,[dim_project].[country] AS [country]
    ,[dim_project].[city] AS [city]
	,[dim_project].[contractor] 
    ,[to_kpi_vAll].[contract_amount] AS [contract_value]
    ,'TRY' AS [contract_value_currency] -- A3'ten gelen tüm projelerde sabit bir currency olduğu için sabit girilmiştir.
    ,[to_kpi_vAll].[contract_type] 
    ,[to_kpi_vAll].[employer] AS [ownership]
	,[dim_project].[sector]  
    ,[to_kpi_vAll].[site_delivery_date] AS [start_date] 
    ,[to_kpi_vAll].[target_finish_date] AS [end_date] 
	,[to_kpi_vAll].[gba]
	,[gba_unit] = 'm²' -- TODO metrekare sabit bir unit mi netleştirilmeli
	,[gba_timestamp] = to_kpi_vAll.[reporting_date]
	,[dim_project].[status] AS [status]
	-- ,[status_timestamp] = 'to be filled' -- TODO status incremental olarak yüklenecek bir sayfada bulunmayacak, o sebeple status timestampi kaldırdık.
	,[to_kpi_vAll].[time_progress]
	,[to_kpi_vAll].[reporting_date]  AS [time_progress_timestamp]
	,[to_kpi_vAll].[physical_progress]
	,[to_kpi_vAll].[reporting_date] AS [physical_progress_timestamp]
	,[to_kpi_vAll].[planned_progress]
	,[to_kpi_vAll].[reporting_date] AS [planned_progress_timestamp]
	,CASE 
		WHEN [to_kpi_vAll].[physical_progress] IS NULL OR [to_kpi_vAll].[planned_progress] IS NULL THEN NULL
		WHEN [to_kpi_vAll].[physical_progress]<[to_kpi_vAll].[planned_progress] THEN 1
		ELSE 0
		END AS is_behind_schedule
	,[to_kpi_vAll].[reporting_date] AS [is_behind_schedule_timestamp]
	,[to_kpi_vAll].[renaissance_direct_personnel]
	,[to_kpi_vAll].[renaissance_indirect_personnel]
	,[to_kpi_vAll].[renaissance_support_personnel]
	,[to_kpi_vAll].[subcontractor_direct_personnel]
	,[to_kpi_vAll].[subcontractor_indirect_personnel]
	,[to_kpi_vAll].[subcontractor_support_personnel]
	,[to_kpi_vAll].[total_personnel] AS [employee_count]
	,[to_kpi_vAll].[reporting_date] AS [employee_count_timestamp]
	--,is_only_rmore
	,is_rmore = CASE WHEN [to_kpi_vAll].[project_id] = 'TO_0000' THEN '1' 
					 WHEN [to_kpi_vAll].[reporting_date] IS NOT NULL THEN '1' 
				ELSE '0' END
	,is_powerbi = CASE WHEN [to_kpi_vAll].[project_id] = 'TO_0000' THEN '0' 
						WHEN [to_kpi_vAll].[reporting_date] IS NOT NULL THEN '1' 
				ELSE '0' END
FROM (
			SELECT *
			FROM (
	  			SELECT 
				*
				,ROW_NUMBER() OVER(PARTITION BY project_id ORDER BY reporting_date DESC) AS last_data_entry_finder
			FROM {{ ref('dm__to_kpi_t_dim_all') }}
			) duplicate_remover_helper
			WHERE duplicate_remover_helper.last_data_entry_finder = 1
		) to_kpi_vAll 
	FULL OUTER JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} as [dim_project]	ON [dim_project].project_id = to_kpi_vAll.project_id
WHERE [dim_project].is_only_rmore = 0 OR dim_project.project_id = 'TO_0000'

UNION ALL
-- Sadece IFC verisi olan, power bi raporlarında yer almayacak ancak rmore'da yer alacak projeler için is_only_rmore kolonuna göre bir union all yapılmıştır:

SELECT 
	[dim_project].[region] AS rls_region
	,CONCAT(COALESCE([dim_project].[group],''),'_',COALESCE([dim_project].[region],'')) AS rls_group
	,CONCAT(COALESCE([dim_project].[company],''),'_',COALESCE([dim_project].[region],'')) AS rls_company
	,CONCAT(COALESCE([dim_project].[business_area],''),'_',COALESCE([dim_project].[region],'')) AS rls_businessarea
	,NULL
	,[project_id]
	,[business_area]
	,[risk_portal_project_id]
	,[res_project_id]
	,CAST([rsafe_id] AS VARCHAR) AS [rsafe_id]
	,[group]
	,[company]
	,[project_name] AS [name]
	,[project_shortname] AS [project_shortname]
	,[latitude] 
	,[longitude] 
	,[country] AS [country]
	,[city] AS [city]
	,[contractor] 
	,NULL,NULL,NULL,NULL
	,[sector]  
	,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
	--is_only_rmore
	,is_rmore = '1'
	,is_powerbi = '0'
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} [dim_project]
WHERE is_only_rmore = 1 AND project_id <> 'TO_0000'