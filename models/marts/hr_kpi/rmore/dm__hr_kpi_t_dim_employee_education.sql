{{
  config(
    materialized = 'table',tags = ['hr_kpi','rmore']
    )
}}

/* 
Date: 20250915
Creator: Adem Numan Kaya
Report Owner: Adem Numan Kaya
Explanation: Yeni SF uzerinden egitim bilgilerinin alindigi rapordur. Eski sistemin bilgileri bulk olarak iceri atildigi icin tek bir sistem uzerinden ilerlenmistir. 
*/


SELECT
	rls.[rls_businessarea]
	,rls.[rls_company]
	,rls.[rls_group]
	,rls.[rls_region]
	,rls.[name_surname]
	,education.[user_id]
	,country_of_education = [country_tr]
	,graduation_status = [graduation_en]
	,level_of_education_txt= [level_of_education_name_en] 
	,name_of_school_tr = name_of_school_tr
	,[education_start_date] = CAST([start_date] AS DATE) 
	, [educaiton_end_date] = CAST([end_date] AS DATE)
	,[degree] as graduation_score
	,[level_of_education]
  FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employee_education') }} as education
  LEFT JOIN {{ ref('stg__hr_kpi_t_dim_sf_rls') }} AS rls 
  	ON rls.[user_id]=education.[user_id]
  WHERE rls_businessarea IS NOT NULL
