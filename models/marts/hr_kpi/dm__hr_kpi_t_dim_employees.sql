{{
  config(
    materialized = 'table',tags = ['hr_kpi','employees']
    )
}}
SELECT 
   rls.rls_region
  ,rls.rls_group
  ,rls.rls_company
  ,rls.rls_businessarea 
  ,emp.[user_id]
  ,CASE
      WHEN emp.[employee_status_tr] = 'Terminated' Then N'ÇIKARILMIŞ' 
      ELSE emp.[employee_status_tr] 
   END as employee_status
  ,emp.[hay_kademe]
  ,emp.[ronesans_rank_personal_tr] as ronesans_rank_personal
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
  ,emp.[position_group]
  ,emp.[workplace_tr] as workplace
  ,emp.job_start_date
  ,emp.job_end_date
  ,emp.seniority_base_date
  ,CASE 
    WHEN emp.gender ='M' THEN 'ERKEK'                                                                                                                     
    WHEN emp.gender = 'F' THEN N'KADIN'
  END AS gender
  ,CONCAT(mng.[name],' ',mng.[surname]) manager_name_surname
  ,emp.actual_location_code
  ,emp.actual_location_name_tr
  ,emp.employee_type_name_en
  ,emp.real_termination_reason_tr
  ,emp.real_termination_reason_en
  ,emp.actual_working_country
  ,emp.job_family
  ,emp.marital_status_name_tr as maritial_status
  ,emp.meal_allowance_tr as meal_allowance_tr
  ,emp.hr_responsible_sf_id
  ,CONCAT(hr.[name],' ',hr.[surname]) hr_business_partner_name_surname
  ,emp.job_function
  ,emp.position_city
  ,emp.employee_city_tr as employee_city
  ,emp.peer_grup
  ,emp.kf_job_subfunction_code
  ,[unit] =
      CASE
          WHEN e.name_tr <> '' THEN e.name_tr
          WHEN d.name_tr <> '' THEN d.name_tr
          WHEN c.name_tr <> '' THEN c.name_tr
          WHEN b.name_tr <> '' THEN b.name_tr
          WHEN a.name_tr <> '' THEN a.name_tr    
      END
  ,emp.[employee_group_code]
  ,emp.sf_system
FROM {{ ref('stg__hr_kpi_t_dim_employees_union_raw') }} emp
	LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levela_union') }} a ON a.code = emp.[a_level_code]
	LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levelb_union') }} b ON b.code = emp.[b_level_code]
	LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levelc_union') }} c ON c.code = emp.[c_level_code]
	LEFT JOIN {{ ref('stg__hr_kpi_t_dim_leveld_union') }} d ON d.code = emp.[d_level_code]
	LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levele_union') }} e ON e.code = emp.[e_level_code]
	LEFT JOIN {{ ref('stg__hr_kpi_t_dim_employees_union_raw') }} mng on emp.manager_user_id = mng.[user_id]
	LEFT JOIN {{ ref('stg__hr_kpi_t_dim_employees_union_raw') }} hr on emp.hr_responsible_sf_id = hr.[user_id]
  LEFT JOIN {{ ref('stg__hr_kpi_t_dim_sf_rls') }} rls on rls.user_id = emp.user_id