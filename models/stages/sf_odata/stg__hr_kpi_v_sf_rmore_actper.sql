{{
  config(
    materialized = 'view',tags = ['uygulama_gelistirme','actper','ACTPERtest', 'hr_kpi'],grants = {'select': ['s4hana_ug_user']}
    )
}}

SELECT [seq_number]
      ,[start_date]
      ,[user_id]
      ,[payroll_company_code]
      ,[payroll_company]
      ,[cost_center_code]
      ,[cost_center]
      ,[country_code]
      ,[country]
      ,[a_level_group]
      ,[employee_area_code] = CASE WHEN [employee_area_code] = 'None' THEN '' ELSE [employee_area_code] END
      ,[employee_area] = CASE WHEN [employee_area] = 'None' THEN '' ELSE [employee_area] END
      ,[unit_code] = CASE WHEN [unit_code] = 'None' THEN '' ELSE [unit_code] END
      ,[unit] = CASE WHEN [unit] = 'None' THEN '' ELSE [unit] END
      ,[employee_group_code]
      ,[employee_group_tr] as [employee_group]
      ,[employee_sub_group_code] = CASE WHEN [employee_sub_group_code] = 'None' THEN '' ELSE [employee_sub_group_code] END 
      ,[employee_sub_group] = CASE WHEN [employee_sub_group_tr] = 'None' THEN '' ELSE [employee_sub_group_tr] END
      ,[employee_sub_group_category] = N'Beyaz Yaka'
	    ,[employee_sub_group_category_code] = '001'
      ,[termination_date] = CASE WHEN [termination_date] = '1753-01-01 00:00:00.000' THEN NULL ELSE [termination_date] END
      ,[initial_hire_date] = CASE WHEN [initial_hire_date] = '1753-01-01 00:00:00.000' THEN NULL ELSE [initial_hire_date] END
      ,[seniority_base_date] = CASE WHEN cast(seniority_base_date as date) = '1753-01-01' THEN NULL ELSE cast(cast(seniority_base_date as date) as datetime) END
      ,[hire_date]
      ,[adins_no]
      ,[global_id]
      ,[sap_id]
      ,[birth_date]
      ,[domain_user]
      ,[name]
      ,[gender]
      ,[surname]
      ,[email_address] = CASE WHEN [email_address] = 'None' THEN '' ELSE [email_address] END
      ,[employee_national_id] = CASE WHEN [employee_national_id] = 'None' THEN '' 
                                    WHEN [employee_national_id] = '0000330000000 00' THEN ''
                                ELSE [employee_national_id] END
      ,[employee_status_tr] as [employee_status]
      ,[job_code]
      ,[job_description]
      ,[employee_sub_area_code] = CASE WHEN [employee_sub_area_code] = 'None' THEN '' ELSE [employee_sub_area_code] END
      ,[employee_sub_area] = CASE WHEN [employee_sub_area] = 'None' THEN '' ELSE [employee_sub_area] END
      ,[second_manager_employee_id] = CASE WHEN [second_manager_employee_id] = 'None' THEN '' ELSE [second_manager_employee_id] END
      ,[second_namager_name] = CASE WHEN [second_manager_name] = 'None' THEN '' ELSE [second_manager_name] END
      ,[second_manager_surname] = CASE WHEN [second_manager_surname] = 'None' THEN '' ELSE [second_manager_surname] END
      ,[second_manager_global_id] = CASE WHEN [second_manager_global_id] = 'None' THEN '' ELSE [second_manager_global_id] END
      ,[second_manager_sap_id] = CASE WHEN [second_manager_sap_id] = 'None' THEN '' ELSE [second_manager_sap_id] END
      ,[manager_global_id] = CASE WHEN [manager_global_id] = 'None' THEN '' ELSE [manager_global_id] END
      ,[manager_sap_id] = CASE WHEN [manager_sap_id] = 'None' THEN '' ELSE [manager_sap_id] END
      ,[manager_employee_id] = CASE WHEN [manager_employee_id] = 'None' THEN '' ELSE [manager_employee_id] END
      ,[manager_name] = CASE WHEN [manager_name] = 'None' THEN '' ELSE [manager_name] END
      ,[manager_surname] = CASE WHEN [manager_surname] = 'None' THEN '' ELSE [manager_surname] END
      ,[position_code] = CASE WHEN [position_code] = 'None' THEN '' ELSE [position_code] END
      ,[position] = CASE WHEN [position] = 'None' THEN '' ELSE [position] END
      ,[functional_manager_employee_id] = CASE WHEN [functional_manager_employee_id] = 'None' THEN '' ELSE [functional_manager_employee_id] END
      ,[functional_manager_global_id] = CASE WHEN [functional_manager_global_id] = 'None' THEN '' ELSE [functional_manager_global_id] END
      ,[functional_manager_sap_id] = CASE WHEN [functional_manager_sap_id] = 'None' THEN '' ELSE [functional_manager_sap_id] END
      ,[job_level]
      ,[functional_namager_name] = CASE WHEN [functional_manager_name] = 'None' THEN '' ELSE [functional_manager_name] END
      ,[functional_manager_surname] = CASE WHEN [functional_manager_surname] = 'None' THEN '' ELSE [functional_manager_surname] END
      ,[duty_field_type] = CASE WHEN [duty_field_type] = 'None' THEN '' ELSE [duty_field_type] END
      ,[duty_field] = CASE WHEN [duty_field] = 'None' THEN '' ELSE [duty_field] END
      ,[business_area_code] = CASE WHEN [business_area_code] = 'None' THEN '' ELSE [business_area_code] END
      ,[business_area] = CASE WHEN [business_area] = 'None' THEN '' ELSE [business_area] END
      ,[hr_responsible_global_id] = CASE WHEN [hr_responsible_global_id] = 'None' THEN '' ELSE [hr_responsible_global_id] END
      ,[hr_responsible_sf_id]= CASE WHEN [hr_responsible_sf_id] = 'None' THEN '' ELSE [hr_responsible_sf_id] END 
      ,[hr_responsible_name] = CASE WHEN [hr_responsible_name] = 'None' THEN '' ELSE [hr_responsible_name] END
      ,[hr_responsible_surname] = CASE WHEN [hr_responsible_surname] = 'None' THEN '' ELSE [hr_responsible_surname] END
      ,[job_code_custom_text] = CASE WHEN [job_code_custom_text] = 'None' THEN '' ELSE [job_code_custom_text] END
      ,[job_code_custom_letter] = CASE WHEN [job_code_custom_text] = 'None' THEN '' ELSE LEFT(job_code_custom_text,1) END
      ,[position_group] = CASE WHEN [position_group] = 'None' THEN '' ELSE [position_group] END
      ,[db_upload_timestamp]
from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_activepersonnel') }}
where 1=1
	and [user_id] not in ('GLB10140','7088') 
  and [global_id] <> ''