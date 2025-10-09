{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}

WITH all_hr_data AS (
	SELECT 	
		FORMAT(created_date, 'yyyy-MM') AS year_month
		,payroll_company_code
	FROM {{ ref('dm__hr_kpi_t_dim_snapshots') }}
	WHERE 1=1
		AND language = 'EN'
		AND employee_status = '663908'
	UNION ALL
	SELECT 
		FORMAT(GETDATE(), 'yyyy-MM') AS year_month
		,payroll_company_code
	FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
	where 1=1
		AND language = 'EN'
		AND employee_status = 'A' 
)
 
SELECT 
    year_month,
    payroll_company_code AS company,
    COUNT(*) AS total_count
FROM all_hr_data
GROUP BY year_month, payroll_company_code