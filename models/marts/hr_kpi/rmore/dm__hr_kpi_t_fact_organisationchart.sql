{{
  config(
    materialized = 'table',tags = ['hr_kpi']

    )
}}

WITH PHOTO_CTE as (
	SELECT  
		[user_id]
		,[photo]
		,ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY WIDTH DESC) rn
	FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employee_photo') }} 
  WHERE LEN([photo]) < 65000
    and LEN([photo]) > 0
  )

	SELECT 
		rls_region = rls.[rls_region]
		,rls_group = rls.[rls_group]
		,rls_company = rls.[rls_company]
		,rls_businessarea = rls.[rls_businessarea]
    ,emp.user_id
    ,emp.[name]
    ,emp.[surname]
    ,[manager_employee_id] = emp.manager_user_id
    ,emp.[position]
    ,picture= pc.photo
	FROM {{ source('stg_sf_odata','raw__hr_kpi_t_sf_employees') }} AS emp 
    LEFT JOIN {{ ref('stg__hr_kpi_t_dim_sf_rls') }} AS RLS ON rls.user_id=emp.user_id
    LEFT JOIN photo_cte pc on pc.user_id = emp.user_id and pc.rn = 1
  WHERE emp.user_id IS NOT NULL
    AND emp.employee_status_tr = N'AKTİF'


