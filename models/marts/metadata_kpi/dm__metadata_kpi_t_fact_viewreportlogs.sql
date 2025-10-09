{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}


with UNIONIZED_LOGS as (

  SELECT 
    [id]
    ,creation_time
    ,[user_id]
    ,[workspace_id]
    ,[report_id]
    ,[report_type]
    ,[consumption_method]
    ,1 AS transaction_amount
    ,report_name
  FROM {{ ref('stg__powerbi_kpi_t_fact_viewreportlogs') }}
  
  UNION ALL
  
  SELECT 
      [log_id] as [id]
      ,[creation_time] as creation_time
      ,LOWER(REPLACE(REPLACE(REPLACE(REPLACE(user_id, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), CHAR(160), '')) AS [user_id]
      ,'RNET' AS [workspace_id]
      ,[report_id]
      ,'RNET' AS [report_type]
      ,'WEB' AS [consumption_method]
      ,1 AS transaction_amount
      ,report_name
  FROM {{ ref('stg__rnet_kpi_t_fact_viewreportlogs') }}

  UNION ALL 

  SELECT *
  FROM {{ ref('stg__s4_kpi_t_fact_viewreportlogs') }}
  UNION ALL 

  SELECT *
  FROM {{ ref('stg__rmore_kpi_t_fact_viewreportlogs') }}

)

SELECT 
  rls_region = CASE 
					WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_en) = 'TR' THEN 'TUR'
					ELSE 'RUS' 
				 END
	,rls_group = CONCAT(UPPER((SELECT TOP 1 group_rls FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_en)),'_',
						CASE 
							WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_en) = 'TR' THEN 'TUR'
							ELSE 'RUS' 
							END
						)
	,rls_company = CONCAT(UPPER(level_b.name_en),'_',
							CASE 
								WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_en) = 'TR' THEN 'TUR'
								ELSE 'RUS' 
							END
							)
	,rls_businessarea = CONCAT(UPPER(emp.business_area),'_',
								CASE 
									WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_en) = 'TR' THEN 'TUR'
									ELSE 'RUS' 
								END
								)
    ,[id]
    ,creation_time
    ,l.[user_id]
    ,CONCAT(emp.[name], ' ', emp.[surname]) AS name_surname
    ,[workspace_id]
    ,[report_id]
    ,[report_name]
    ,[report_type]
    ,[consumption_method]
    ,transaction_amount
FROM UNIONIZED_LOGS l
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} emp ON emp.email_address = l.user_id
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_a') }} level_a ON level_a.code = emp.a_level_code
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_b') }} level_b ON level_b.code = emp.b_level_code