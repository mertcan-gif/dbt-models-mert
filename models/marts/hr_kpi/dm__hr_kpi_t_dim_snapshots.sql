{{
  config(
    materialized = 'table',tags = ['hr_kpi','hr_snapshots']
    )
}}

WITH SICIL_RLS_MATCHING AS (
	SELECT *
	FROM (
		SELECT 
			rls_region
			,rls_group
			,rls_company
			,rls_businessarea
			,sap_id
			,ROW_NUMBER() OVER(PARTITION BY sap_id ORDER BY event_reason,age DESC) AS RN	
		FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
		WHERE language = 'EN'
	) RAW_DATA
	WHERE RAW_DATA.RN = 1
)

,hr_beyaz_yaka_previous AS (

	SELECT [rls_region]
		,[rls_group]
		,[rls_company]
		,[rls_businessarea] 
		,ps.*
		,ROW_NUMBER() OVER(PARTITION BY ps.sap_id, ps.created_date ORDER BY ps.age DESC) rn
	FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_previoussnapshots') }} ps
			LEFT JOIN SICIL_RLS_MATCHING srm ON srm.sap_id = ps.sap_id
	WHERE 1=1
			and language = 'EN' 
			and ps.sap_id IS NOT NULL
			and cast(created_date AS date) < '2023-09-01'
)

,hr_beyaz_yaka AS (

	SELECT 
		LI.[rls_region],
		LI.[rls_group],
		LI.[rls_company],
		LI.[rls_businessarea],
		LD.[dwh_data_group],
		[employee_status] = CASE WHEN LI.[employee_status] = 'A' THEN '663908' ELSE '663918' END,
		LD.[sf_id_number],
		LI.[employee_id],
		LI.[adines_number],
		[global_id] = CAST(LI.[global_id] AS nvarchar),
		LI.[sap_id],
		LI.[username],
		LD.[name],
		[surname] = LD.[surname] collate database_default,
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
	FROM {{ ref('stg__hr_kpi_v_dim_languageindependentsnapshots') }} LI 
		LEFT JOIN {{ ref('stg__hr_kpi_v_dim_languagedependentsnapshots') }} LD  on LI.sf_id_number = LD.sf_id_number
                                                                           and LI.snapshot_date = LD.snapshot_date
)

,UNIONIZED_SNAPSHOTS AS (
	SELECT hr.* , rn = ''
	FROM hr_beyaz_yaka hr
	WHERE 1=1
		AND custom_region <> 'RU'
		AND collar_type <> 'BLUE COLLAR'
		AND language = 'EN'
		AND sap_id IS NOT NULL

	UNION ALL 

	SELECT hrp.*
	FROM hr_beyaz_yaka_previous hrp
	WHERE 1=1 
		AND rn = 1	
		AND custom_region <> 'RU'
		AND collar_type <> 'BLUE COLLAR'
)

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
	,[global_id] = REPLACE([global_id], '.0','')
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
	,[created_date]
FROM UNIONIZED_SNAPSHOTS
   


