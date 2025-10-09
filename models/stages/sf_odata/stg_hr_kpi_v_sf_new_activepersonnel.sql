{{
  config(
    materialized = 'view',tags = ['sf_new_api'],grants = {'select': ['s4hana_ug_user']}
    )
}}

/* Rus personellerin filter out edilmesi gerekecek 
   Datamartı güncellenecek */

SELECT 
       emp.[seq_number]
      ,emp.[start_date]
      ,emp.[user_id]
      ,emp.[payroll_company_code]
      ,emp.[payroll_company]
      ,emp.[cost_center_code]
      ,emp.[cost_center_name] as cost_center
      ,emp.[country_code]
      ,emp.[country]
      ,level_a_table.name_tr as [a_level_group]
      ,[employee_area_code] = CASE WHEN emp.[employee_area_code] = 'None' THEN '' ELSE emp.[employee_area_code] END
	  ,[employee_area] = CASE WHEN emp.[employee_area_en] = 'None' THEN '' ELSE emp.[employee_area_en] END
	  ,[unit_code] = CASE WHEN emp.[unit_code] = 'None' THEN '' ELSE emp.[unit_code] END
      ,[unit] =
            CASE
                WHEN e_level.name_tr <> '' THEN e_level.name_tr
                WHEN d_level.name_tr <> '' THEN d_level.name_tr
                WHEN c_level.name_tr <> '' THEN c_level.name_tr
                WHEN b_level.name_tr <> '' THEN b_level.name_tr
                WHEN level_a_table.name_tr <> '' THEN level_a_table.name_tr    
            END
      ,emp.[employee_group_code]
      ,emp.[employee_group_tr] as [employee_group]
      ,[employee_sub_group_code] = CASE WHEN emp.[employee_sub_group_code] = 'None' THEN '' ELSE emp.[employee_sub_group_code] END 
      ,[employee_sub_group] = CASE WHEN emp.[employee_sub_group_tr] = 'None' THEN '' ELSE emp.[employee_sub_group_tr] END
      ,[employee_sub_group_category] = N'Beyaz Yaka'
	  ,[employee_sub_group_category_code] = '001'
      ,[termination_date] = CASE WHEN emp.[job_end_date] = '1753-01-01 00:00:00.000' THEN NULL ELSE  emp.[job_end_date]  END 
      ,[initial_hire_date] = CASE WHEN emp.[initial_hire_date] = '1753-01-01 00:00:00.000' THEN NULL ELSE emp.[initial_hire_date] END
      ,[seniority_base_date] = CASE WHEN cast(emp.seniority_base_date as date) = '1753-01-01' THEN NULL ELSE cast(cast(emp.seniority_base_date as date) as datetime) END
      ,[hire_date] = emp.job_start_date
	  ,[adins_no] = NULL -- yeni sistemde adins_no yok
      ,emp.[global_id]
      ,emp.[sap_id]
      ,emp.[date_of_birth] as [birth_date]
      ,emp.[domain_user]
      ,emp.[name]
      ,emp.[gender]
      ,emp.[surname]
      ,[email_address] = CASE WHEN emp.[email_address] = 'None' THEN '' ELSE emp.[email_address] END
      ,[employee_national_id] = CASE WHEN emp.[employee_national_id] = 'None' THEN '' 
                                    WHEN emp.[employee_national_id] = '0000330000000 00' THEN ''
                                ELSE emp.[employee_national_id] END
      ,case 
        when emp.[employee_status_tr] = 'Terminated' THEN N'ÇIKARILMIŞ'
        ELSE UPPER(emp.[employee_status_tr])
        END  as [employee_status]
      ,emp.[job_code]
      ,emp.[job_description]
      ,[employee_sub_area_code] = CASE WHEN emp.[employee_sub_area_code] = 'None' THEN '' ELSE emp.[employee_sub_area_code] END
      ,[employee_sub_area] = CASE WHEN emp.[employee_sub_area] = 'None' THEN '' ELSE emp.[employee_sub_area] END
      ,[second_manager_employee_id] = CASE WHEN emp.[second_manager_user_id] = 'None' THEN '' ELSE emp.[second_manager_user_id] END
      ,[second_manager_name] = CASE WHEN emp.[second_manager_name] = 'None' THEN '' ELSE emp.[second_manager_name] END
      ,[second_manager_surname] = CASE WHEN emp.[second_manager_surname] = 'None' THEN '' ELSE emp.[second_manager_surname] END
      ,[second_manager_global_id] = CASE WHEN emp.[second_manager_global_id] = 'None' THEN '' ELSE emp.[second_manager_global_id] END
      ,[second_manager_sap_id] = CASE WHEN emp.[second_manager_sap_id] = 'None' THEN '' ELSE emp.[second_manager_sap_id] END
      ,[manager_global_id] = CASE WHEN emp.[manager_global_id] = 'None' THEN '' ELSE emp.[manager_global_id] END
      ,[manager_sap_id] = CASE WHEN emp.[manager_sap_id] = 'None' THEN '' ELSE emp.[manager_sap_id] END
      ,[manager_employee_id] = CASE WHEN emp.[manager_user_id] = 'None' THEN '' ELSE emp.[manager_user_id] END 
      ,[manager_name] = CASE WHEN emp.[manager_name] = 'None' THEN '' ELSE emp.[manager_name] END
      ,[manager_surname] = CASE WHEN emp.[manager_surname] = 'None' THEN '' ELSE emp.[manager_surname] END
      ,[position_code] = CASE WHEN emp.[position_code] = 'None' THEN '' ELSE emp.[position_code] END
      ,[position] = CASE WHEN emp.[position] = 'None' THEN '' ELSE emp.[position] END
      ,[functional_manager_employee_id] = CASE WHEN emp.[functional_manager_employee_id] = 'None' THEN '' ELSE emp.[functional_manager_employee_id] END
      ,[functional_manager_global_id] = CASE WHEN emp.[functional_manager_global_id] = 'None' THEN '' ELSE emp.[functional_manager_global_id] END
      ,[functional_manager_sap_id] = CASE WHEN emp.[functional_manager_sap_id] = 'None' THEN '' ELSE emp.[functional_manager_sap_id] END
      ,emp.[job_level]
      ,[functional_manager_name] = CASE WHEN emp.[functional_manager_name] = 'None' THEN '' ELSE emp.[functional_manager_name] END
      ,[functional_manager_surname] = CASE WHEN emp.[functional_manager_surname] = 'None' THEN '' ELSE emp.[functional_manager_surname] END
	  ,[duty_field_type] = emp.actual_location_name_en
	  ,[duty_field] = emp.actual_location_code
      ,[business_area_code] = CASE WHEN emp.[business_area_code] = 'None' THEN '' ELSE emp.[business_area_code] END
      ,[business_area] = CASE WHEN emp.[business_area] = 'None' THEN '' ELSE emp.[business_area] END
      ,[hr_responsible_global_id] = CASE WHEN emp.[hr_responsible_global_id] = 'None' THEN '' ELSE emp.[hr_responsible_global_id] END
      ,[hr_responsible_name] = CASE WHEN emp.[hr_responsible_name] = 'None' THEN '' ELSE emp.[hr_responsible_name] END
      ,[hr_responsible_surname] = CASE WHEN emp.[hr_responsible_surname] = 'None' THEN '' ELSE emp.[hr_responsible_surname] END
	  ,[job_code_custom_text] = ''
	  ,[job_code_custom_letter] = ''
      ,[position_group] = CASE WHEN emp.[position_group] = 'None' THEN '' ELSE emp.[position_group] END
      ,emp.[db_upload_timestamp]
from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} emp
    LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_a') }} level_a_table ON emp.a_level_code = level_a_table.code
    LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_b') }} b_level ON b_level.code = emp.[b_level_code]
    LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_c') }} c_level ON c_level.code = emp.[c_level_code]
    LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_d') }} d_level ON d_level.code = emp.[d_level_code]
    LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_e') }} e_level ON e_level.code = emp.[e_level_code]
where 1=1
	and emp.[user_id] not in ('GLB10140','7088') -- Ozan Bey'in isteği üzerine filtredeki userID kaldırılmıştır. 
    and emp.[global_id] <> ''