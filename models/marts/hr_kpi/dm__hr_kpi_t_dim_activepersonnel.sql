{{
  config(
    materialized = 'table',tags = ['hr_kpi','hr_activepersonnel']
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
			,sf_id_number
			,ROW_NUMBER() OVER(PARTITION BY sf_id_number ORDER BY event_reason,age DESC) AS RN	
		FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
		WHERE language = 'EN'
	) RAW_DATA
	WHERE RAW_DATA.RN = 1
)

SELECT 
	S.rls_region
	,S.rls_group
	,S.rls_company
	,S.rls_businessarea
	,A.*
FROM {{ ref('stg_hr_kpi_v_sf_personnellist') }} A
	LEFT JOIN SICIL_RLS_MATCHING S ON S.sf_id_number = A.user_id
WHERE S.sf_id_number IS NOT NULL
