{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}



WITH CTE AS (
		SELECT 	  
			   sf_system = 'Coach' 
			  ,emp.[seq_number]
			  ,emp.[start_date]
			  ,emp.[user_id]
			  ,IIF(emp.[end_date]= '9999-12-31 00:00:00+00:00', '2025-06-30 00:00:00+00:00',emp.end_date)  as end_date --sf geçişindeki tarih
			  ,emp.[job_end_date]
			  ,emp.[employee_status_tr]
			  ,emp.[employee_status_en]
			  ,emp.[hay_kademe]
			  ,emp.[hay_kademe_personal]
			  ,emp.[ronesans_rank_personal]
			  ,emp.[ronesans_rank_tr]
			  ,emp.[ronesans_rank_en]
			  ,emp.[global_id]
			  ,emp.[sap_id]
			  ,emp.[name]
			  ,emp.[surname]
			  ,emp.[a_level_code]
			  ,emp.[b_level_code]
			  ,emp.[c_level_code]
			  ,emp.[d_level_code]
			  ,emp.[e_level_code]
			  ,emp.[position]
			  ,emp.[business_function]
			  ,emp.[cost_center_code]
			  ,emp.[cost_center_name]
			  ,emp.[payroll_company_code]
			  ,emp.[payroll_company]
			  ,[manager_user_id] = emp_singular.sap_id
			  ,emp.[workplace_tr]
			  ,emp.[workplace_en]
			  ,emp.[job_start_date]
			  ,emp.[business_area]
			  ,emp.[email_address]
			  ,emp.[date_of_birth]
			  ,emp.[employee_type_tr]
			  ,emp.[employee_type_en]
			  ,emp.[gender]
			  ,emp.[total_team_size]
			  ,emp.[team_member_size]
			  ,emp.[actual_working_country]
			  ,emp.[physical_location]
			  ,emp.[physical_location_city]
			  ,emp.[real_termination_reason_tr]
			  ,emp.[real_termination_reason_en]
			  ,emp.[event_reason]
			  ,emp.[db_upload_timestamp]
		FROM  {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees_historia') }} emp
			left join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }} emp_singular on emp.manager_user_id = emp_singular.user_id
		where 1=1
			and emp.employee_status_en = 'Active'
		UNION ALL
		SELECT 
			  sf_system = 'Rpeople' 
			  ,[seq_number]
			  ,hst.[start_date]
			  ,hst.[user_id]
			  ,end_date --sf geçişindeki tarih
			  ,hst.[job_end_date]
			  ,hst.[employee_status_tr]
			  ,hst.[employee_status_en]
			  ,hst.[hay_kademe] 
			  ,hst.[hay_kademe] AS [hay_kademe_personal]
			  ,hst.[ronesans_rank_personal_tr] AS [ronesans_rank_personal]
			  ,hst.[ronesans_rank_tr]
			  ,hst.[ronesans_rank_en]
			  ,hst.[global_id]
			  ,hst.[sap_id]
			  ,hst.[name]
			  ,hst.[surname]
			  ,hst.[a_level_code]
			  ,hst.[b_level_code]
			  ,hst.[c_level_code]
			  ,hst.[d_level_code]
			  ,hst.[e_level_code]
			  ,hst.[position]
			  ,[business_function] = hst.position_group
			  ,hst.[cost_center_code]
			  ,hst.[cost_center_name]
			  ,hst.[payroll_company_code]
			  ,hst.[payroll_company]
			  ,hst.[manager_user_id]
			  ,hst.[workplace_tr]
			  ,hst.[workplace_en]
			  ,hst.[job_start_date]
			  ,hst.[business_area]
			  ,hst.[email_address]
			  ,hst.[date_of_birth]
			  ,[employee_type_tr] = hst.[employee_type_name_tr]
			  ,[employee_type_en] = hst.[employee_type_name_en]
			  ,hst.[gender]
			  ,hst.[total_team_size]
			  ,hst.[team_member_size]
			  ,hst.[actual_working_country]
			  ,[physical_location] = hst.[actual_location_name_tr]
			  ,[physical_location_city] = hst.[position_city_tr]
			  ,hst.[real_termination_reason_tr]
			  ,hst.[real_termination_reason_en]
			  ,[event_reason] = evnt.name_tr
			  ,hst.[db_upload_timestamp]
		from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees_historia') }} hst
			left join  {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_eventreasons') }} evnt on hst.[eventreason_code] = evnt.code
		where 1=1
			and employee_status_en = 'Active'
			and cast(hst.[start_date] as date )>= '2025-07-01'
	)

SELECT *
FROM CTE
