{{
  config(
    materialized = 'table',tags = ['hr_kpi_draft','hr_snapshots_draft','snapshots_all']
    )
}}

-- select 1 as test

 

WITH hr_beyaz_yaka AS (

	SELECT 
		LI.[rls_region],
		LI.[rls_group],
		LI.[rls_company],
		LI.[rls_businessarea],
		LD.[dwh_data_group],
		LI.[employee_status],
		LD.[sf_id_number],
		LI.[employee_id],
		LI.[adines_number],
		[global_id] = CAST(LI.[global_id] AS nvarchar),
		LI.[sap_id],
		LI.[username],
		LD.[name],
		LD.[surname],
		LD.[full_name],
		LI.[gender],
		LI.[marital_status],
		LI.[nationality],
		LD.[event_reason],
		LI.[country],
		LD.[payroll_company],
		LD.[cost_center],
		LD.[employee_group],
		LI.[role_code],
		LD.[role],
		LI.[ronesans_job_level],
		LI.[supervisor_name],
		LI.[date_of_birth],
		LI.[age],
		LD.[language],
		LD.[a_level_group],
		LD.[b_level_company],
		LD.[c_level_region],
		LD.[d_level_department],
		LD.[e_level_unit],
		LI.[custom_region],
		LI.[actual_working_country],
		LI.[actual_working_city],
		LD.[collar_type],
		LI.[dwh_leave],
		LI.[dwh_annual_leave],
		LI.[dwh_used_leave],
		LD.[dwh_workplace],
		LI.[dwh_origin_code],
		LI.[dwh_type],
		LI.[dwh_employee_type],
		LD.[dwh_education_status],
		LI.[dwh_date_of_recruitment],
		LI.[dwh_date_of_termination],
		LI.[ronesans_rank],
		LI.[grouped_title],
		LI.[dwh_termination_reason],
		LI.[dwh_cause_details],
		LI.[dwh_ronesans_last_seniority],
		LI.[dwh_ronesans_seniority],
		LI.[dwh_non_ronesans_seniority],
		LI.[dwh_ronesans_total_seniority],
		LI.[dwh_actual_termination_reason],
		LI.[dwh_worksite],
		LI.[payroll_sub_unit],
		LI.[dwh_worksite_description],
		LD.[dwh_workplace_merged],
		LI.[payroll_company_code],
		created_date = LI.[snapshot_date]
	FROM {{ ref('stg__hr_kpi_v_dim_languageindependentsnapshots_alldates') }} LI 
		LEFT JOIN {{ ref('stg__hr_kpi_v_dim_languagedependentsnapshots_alldates') }} LD  on LI.sf_id_number = LD.sf_id_number
                                                                           and LI.snapshot_date = LD.snapshot_date
)


SELECT hr.* 
FROM hr_beyaz_yaka hr
WHERE 1=1
	AND custom_region <> 'RU'
  AND collar_type <> 'BLUE COLLAR'
  AND language = 'EN'
  


