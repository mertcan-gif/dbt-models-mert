{{
  config(
    materialized = 'table',tags = ['hr_kpi']
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
			,employee_id
			,ROW_NUMBER() OVER(PARTITION BY employee_id ORDER BY event_reason,age DESC) AS RN	
		FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
		WHERE language = 'EN'
	) RAW_DATA
	WHERE RAW_DATA.RN = 1
)

    SELECT 			   
        srm.rls_region,
        srm.rls_group,
        srm.rls_company,
        srm.rls_businessarea
        ,pt.* 
    FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_personnel_tickets') }} pt
    LEFT JOIN SICIL_RLS_MATCHING srm ON srm.employee_id = pt.user_id
