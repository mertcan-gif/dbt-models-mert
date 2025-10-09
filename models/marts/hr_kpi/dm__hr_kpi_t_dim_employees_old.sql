{{
  config(
    materialized = 'table',tags = ['hr_kpi','employees']
    )
}}
SELECT  
  emp.[seq_number]
  ,emp.[start_date]
  ,emp.[user_id]
  ,emp.[employee_status_tr] as employee_status
  ,emp.[hay_kademe]
  ,emp.[hay_kademe_personal]
  ,emp.[ronesans_rank_personal]
  ,emp.[cost_center_code]
  ,emp.[cost_center_name]
  ,emp.[payroll_company]
  ,emp.[payroll_company_code]
  ,emp.[manager_user_id]
  ,emp.[business_area]
  ,emp.[email_address]
  ,emp.[total_team_size]
  ,emp.[team_member_size]
  ,emp.[date_of_birth]
  ,emp.[ronesans_rank_tr] as ronesans_rank
  ,emp.[global_id]
  ,emp.[sap_id]
  ,emp.[name]
  ,emp.[surname]
  ,CONCAT(emp.[name], ' ', emp.[surname]) AS full_name
  ,a.name_tr AS a_level
  ,b.name_tr AS b_level
  ,c.name_tr AS c_level
  ,d.name_tr AS d_level
  ,e.name_tr AS e_level
  ,emp.[position]
  ,emp.[business_function]
  ,emp.[workplace_tr] as workplace
  ,emp.job_start_date
  ,emp.job_end_date
  ,CASE 
    WHEN emp.gender ='M' THEN 'ERKEK'                                                                                                                     
    WHEN emp.gender = 'F' THEN N'KADIN'
  END AS gender
  ,CONCAT(mng.[name],' ',mng.[surname]) manager_name_surname
  ,emp.physical_location
  ,emp.physical_location_city
  ,emp.employee_type_en
  ,emp.real_termination_reason_tr
  ,emp.real_termination_reason_en
  ,emp.actual_working_country
FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }} emp
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_a') }} a ON a.code = emp.[a_level_code]
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_b') }} b ON b.code = emp.[b_level_code]
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_c') }} c ON c.code = emp.[c_level_code]
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_d') }} d ON d.code = emp.[d_level_code]
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_e') }} e ON e.code = emp.[e_level_code]
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }} mng on emp.manager_user_id = mng.[user_id]