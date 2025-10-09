{{
  config(
    materialized = 'table',tags = ['hr_kpi','normstaff']
    )
}}


SELECT
	rls_region = CASE 
					WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = a_level COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR'
					ELSE 'RUS' 
				END
	,rls_group = CONCAT(
						(SELECT TOP 1 [group_rls] FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = a_level COLLATE DATABASE_DEFAULT) ,'_',
						CASE 
							WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = a_level COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR'
							ELSE 'RUS' 
						END
						)
	,rls_company =  CONCAT(
						'_',
						CASE 
							WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = a_level COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR'
							ELSE 'RUS' 
						END
						)
	,rls_businessarea = CONCAT(
						'_',
						CASE 
							WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = a_level COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR'
							ELSE 'RUS' 
						END
						),
  code,
  job_title,									
  start_date = CAST(effective_start_date AS DATE),	
  end_date = CAST(effective_end_date AS DATE),
  planned_end_date = CAST(planned_end_date AS DATE),									
  is_budgeted = NULL,																	
  company_group = a_level,											
  a_level,										
  b_level,
  c_level,
  d_level,
  e_level,
  gyg_status,
  cast([planned_recruitment_date] as date) as planned_recruitment_date,												
  payroll_company,																
  [status],														
  payroll_company_code = NULL,											
  recruitment_status = NULL,																
  global_id_user = incumbent,												
  current_employee_name =  CASE
  								WHEN UPPER(incumbents_name) = 'NULL NULL' THEN NULL
								ELSE UPPER(incumbents_name) 
							END,	
  cost_center,
  db_upload_timestamp = CAST([db_upload_timestamp] AS DATE)										

FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_positions') }} nk
WHERE 1=1
	AND [status] = 'A' 
	AND NOT (incumbent = '' and is_vacant = 0)

