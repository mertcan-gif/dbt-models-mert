{{
  config(
    materialized = 'table',tags = ['sf_new_api','hrall']
    )
}}
WITH hr_all AS (

	SELECT 
	[rls_region]
      ,[rls_group]
      ,[rls_company]
      ,[rls_businessarea]
      ,[dwh_data_group]
      ,[employee_status]
      ,[sf_id_number]
      ,[employee_id]
      ,[adines_number]
      ,[global_id]
      ,[sap_id]
      ,[username]
      ,[name]
      ,[surname]
      ,[full_name]
      ,[gender]
      ,[marital_status]
      ,[nationality]
      ,[event_reason]
      ,[country]
      ,[payroll_company]
      ,[cost_center]
      ,[employee_group]
      ,[role_code]
      ,[role]
      ,[ronesans_job_level]
      ,[supervisor_name]
      ,[date_of_birth]
      ,[age]
      ,[language]
      ,[a_level_group]
      ,[b_level_company]
      ,[c_level_region]
      ,[d_level_department]
      ,[e_level_unit]
      ,[custom_region]
      ,[actual_working_country]
      ,[actual_working_city]
      ,[collar_type]
      ,[dwh_leave]
      ,[dwh_annual_leave]
      ,[dwh_used_leave]
      ,[dwh_workplace]
      ,[dwh_origin_code]
      ,[dwh_type]
      ,[dwh_employee_type]
      ,[dwh_education_status]
      ,[dwh_date_of_recruitment]
      ,[dwh_date_of_termination]
      ,[ronesans_rank]
      ,[grouped_title]
      ,[dwh_termination_reason]
      ,[dwh_cause_details]
      ,[dwh_ronesans_last_seniority]
      ,[dwh_ronesans_seniority]
      ,[dwh_non_ronesans_seniority]
      ,[dwh_ronesans_total_seniority]
      ,[dwh_actual_termination_reason]
      ,[dwh_worksite]
      ,[payroll_sub_unit]
      ,[dwh_worksite_description]
      ,[dwh_workplace_merged]
      ,[payroll_company_code]
      ,[photo_base64]
      ,employment_type = N'Rönesans'
	FROM {{ ref('stg_hr_kpi_t_sf_new_hrall') }}
	where (calisan_tipi <> 'Ghost User' OR calisan_tipi IS NULL)

)


SELECT 
  *
FROM hr_all hr

/*
UNION ALL

SELECT 
  *
FROM {{ ref('stg__hr_kpi_t_dim_maviyaka_finalized') }} 
*/

