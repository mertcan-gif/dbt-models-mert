{{
  config(
    materialized = 'view',tags = ['sf_new_api'],grants = {'select': ['s4hana_ug_user']}
    )
}}

/* Rus personellerin filter out edilmesi gerekecek 
   Datamartı güncellenecek */


with personnel_list as (
  SELECT 
      sf_system = 'Rpeople'
      ,_key = CONCAT(global_id, sap_id,user_id) ,
      *  
	FROM {{ ref('stg_hr_kpi_v_sf_new_activepersonnel') }}
  WHERE payroll_company_code <> N'veri_aktarimi'
    UNION ALL
  SELECT 
		sf_system = 'Coach'
		, _key = CONCAT(global_id, sap_id,user_id) 
		,*  
	FROM {{ ref('stg__hr_kpi_v_sf_activepersonnel') }}  where employee_status = N'ÇIKARILMIŞ'
  )

SELECT 
  [sf_system]
  ,[seq_number]
  ,[start_date]
  ,[user_id]
  ,[payroll_company_code]
  ,[payroll_company]
  ,[cost_center_code]
  ,[cost_center]
  ,[country_code]
  ,[country]
  ,[a_level_group]
  ,[employee_area_code]
  ,[employee_area]
  ,[unit_code]
  ,[unit]
  ,[employee_group_code]
  ,[employee_group]
  ,[employee_sub_group_code]
  ,[employee_sub_group]
  ,[employee_sub_group_category]
  ,[employee_sub_group_category_code]
  ,[termination_date]
  ,[initial_hire_date]
  ,[seniority_base_date]
  ,[hire_date]
  ,[adins_no]
  ,[global_id]
  ,[sap_id]
  ,[birth_date]
  ,[domain_user]
  ,[name]
  ,[gender]
  ,[surname]
  ,[email_address]
  ,[employee_national_id]
  ,[employee_status]
  ,[job_code]
  ,[job_description]
  ,[employee_sub_area_code]
  ,[employee_sub_area]
  ,[second_manager_employee_id]
  ,[second_manager_name]
  ,[second_manager_surname]
  ,[second_manager_global_id]
  ,[second_manager_sap_id]
  ,[manager_global_id]
  ,[manager_sap_id]
  ,[manager_employee_id]
  ,[manager_name]
  ,[manager_surname]
  ,[position_code]
  ,[position]
  ,[functional_manager_employee_id]
  ,[functional_manager_global_id]
  ,[functional_manager_sap_id]
  ,[job_level]
  ,[functional_manager_name]
  ,[functional_manager_surname]
  ,[duty_field_type]
  ,[duty_field]
  ,[business_area_code]
  ,[business_area]
  ,[hr_responsible_global_id]
  ,[hr_responsible_name]
  ,[hr_responsible_surname]
  ,[job_code_custom_text]
  ,[job_code_custom_letter]
  ,[position_group]
  ,[db_upload_timestamp]
FROM 
	(
	SELECT
		ROW_NUMBER() OVER (PARTITION BY _key ORDER BY sf_system desc) as rn
		,*
	FROM personnel_list) as subq
  where 1=1
    AND rn = 1
    and user_id <> '253' --Seyit Ozan Bey'in isteği doğrultusunda bu kişinin 2 profilinden biri filtrelenmiştir.