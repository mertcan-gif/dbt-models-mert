{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}

SELECT 
	rls.[rls_businessarea]
	,rls.[rls_company]
	,rls.[rls_group]
	,rls.[rls_region]
  ,rls.[name_surname]
	,languages.[user_id]
  ,[language]=languages.[language_tr]
  ,[writing]=languages.[writing_tr]
  ,[reading]=languages.[reading_tr]
  ,[speaking]=languages.[speaking_tr]
  FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_languages') }} as languages
LEFT JOIN {{ ref('stg__hr_kpi_t_dim_sf_rls') }} AS rls on rls.[user_id]=languages.[user_id]
WHERE rls.[rls_businessarea] IS NOT NULL 