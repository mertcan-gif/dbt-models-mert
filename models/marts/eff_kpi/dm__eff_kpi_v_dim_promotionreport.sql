{{
  config(
    materialized = 'view',tags = ['eff_kpi']
    )
}}

SELECT 
	A.*
	,B.[dwh_employee_type]
	,B.[dwh_workplace]
	,B.[dwh_education_status]
	,B.[dwh_ronesans_last_seniority]
FROM {{ ref('dm__hr_kpi_t_dim_promotion') }} A
	LEFT JOIN {{ref( 'dm__hr_kpi_t_dim_hrall') }} B ON A.[sap_id] = B.[sap_id]
WHERE B.[dwh_employee_type] <> 'Pusula'
