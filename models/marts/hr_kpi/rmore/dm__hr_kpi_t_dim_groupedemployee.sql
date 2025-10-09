{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}

SELECT 
	rls_region
	,rls_group
	,rls_company
	,company_code
	,company_name
	,actual_working_country
	,[employment_type_tr]
	,[collar_type]
	,[gender]
	,COUNT(DISTINCT [sf_id_number]) AS employee_count
  FROM {{ ref('stg__hr_kpi_t_dim_employeelist') }}
  GROUP BY 
	rls_region
	,rls_group
	,rls_company
	,company_code
	,company_name
	,actual_working_country
	,[collar_type]
	,[gender]
	,[employment_type_tr]