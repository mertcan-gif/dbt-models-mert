{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}

WITH main_cte AS (
SELECT
   rls_region = 
		CASE 
			WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = a.name_tr COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR'
			ELSE 'RUS' 
		END
  ,rls_group = UPPER((SELECT TOP 1 group_rls FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = a.name_tr))
  ,rls_company = UPPER(b.name_en)
  ,rls_businessarea = UPPER(emp.business_area)
  ,emp.[user_id]
  ,emp.[global_id]
  ,emp.[sap_id]
  ,CONCAT(emp.[name], ' ', emp.[surname]) AS full_name
  ,emp.[email_address]
  ,CASE 
		WHEN emp.gender ='M' THEN 'MALE'                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
		WHEN emp.gender = 'F' THEN N'FEMALE'
   END AS gender
  ,emp.[position]
  ,emp.[business_function]
  ,emp.[payroll_company]
  ,emp.[payroll_company_code]
  ,emp.[workplace_en]
  ,emp.[business_area]
  ,a.name_tr AS a_level
  ,b.name_tr AS b_level
  ,c.name_tr AS c_level
  ,d.name_tr AS d_level
  ,e.name_tr AS e_level
  ,CAST(emp.[job_start_date] AS date) AS job_start_date
  ,CAST(emp.[job_end_date] AS date) AS job_end_date
  ,CAST(emp.[date_of_birth] AS date) AS date_of_birth
  ,CASE
		WHEN YEAR(emp.job_end_date) <> 1753 THEN CAST(ROUND((DATEDIFF(YEAR, emp.job_start_date, emp.job_end_date) + DATEDIFF(MONTH, DATEADD(YEAR, DATEDIFF(YEAR, emp.job_start_date, emp.job_end_date), emp.job_start_date), emp.job_end_date) / 12.0), 1) AS FLOAT)
		ELSE CAST(ROUND((DATEDIFF(YEAR, emp.job_start_date, GETDATE()) + DATEDIFF(MONTH, DATEADD(YEAR, DATEDIFF(YEAR, emp.job_start_date, GETDATE()), emp.job_start_date), GETDATE()) / 12.0), 1) AS FLOAT)
   END AS years_of_service
  ,CASE
		WHEN YEAR(emp.job_end_date) <> 1753 THEN DATEDIFF(DAY, emp.job_start_date, emp.job_end_date)
		ELSE DATEDIFF(DAY, emp.job_start_date, GETDATE())
   END AS days_of_service
  ,CONCAT(mng.[name],' ',mng.[surname]) manager_full_name
  ,emp.[employee_status_en]
  ,emp.[real_termination_reason_en]
  ,emp.[actual_working_country]
  ,emp.[physical_location]
FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }} emp 
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_a') }} a ON a.code = emp.[a_level_code]
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_b') }} b ON b.code = emp.[b_level_code]
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_c') }} c ON c.code = emp.[c_level_code]
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_d') }} d ON d.code = emp.[d_level_code]
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_e') }} e ON e.code = emp.[e_level_code]
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }} mng on emp.manager_user_id = mng.[user_id]
)

SELECT
	 rls_region
	,rls_group = CONCAT(COALESCE([rls_group],''),'_',COALESCE([rls_region],''))
	,rls_company = CONCAT(COALESCE([rls_company],''),'_',COALESCE([rls_region],''))
	,rls_businessarea = CONCAT(COALESCE([rls_businessarea],''),'_',COALESCE([rls_region],''))
	,[user_id]
	,[global_id]
	,[sap_id]
	,[full_name]
	,[email_address]
	,[gender]
	,[position]
	,[employee_status_en]
	,[real_termination_reason_en]
	,[business_function]
	,[payroll_company_code]
	,[payroll_company]
	,[workplace_en]
	,[business_area]
	,[a_level]
	,[b_level]
	,[c_level]
	,[d_level]
	,[e_level]
	,[job_start_date]
	,[job_end_date]
	,[date_of_birth]
	,[years_of_service] AS seniority_in_years
	,CASE 
        	WHEN years_of_service >= 0 AND years_of_service <= 3 THEN '0-3'
        	WHEN years_of_service > 3 AND years_of_service <= 5  THEN '3-5'
        	WHEN years_of_service > 5 AND years_of_service <= 7  THEN '5-7'
        	WHEN years_of_service > 7 AND years_of_service <= 10 THEN '7-10'
        	WHEN years_of_service > 10 THEN '10+' 
	END AS seniority_level
	,[days_of_service] AS seniority_in_days
	,RANK() OVER(PARTITION BY a_level, b_level ORDER BY days_of_service DESC) AS seniority_rank_by_company
	,[manager_full_name]
FROM main_cte
