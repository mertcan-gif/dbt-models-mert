{{
  config(
    materialized = 'table',tags = ['hr_kpi_draft']
    )
}}

SELECT
	rls_region = CASE 
					WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_tr) = 'TR' THEN 'TUR'
					ELSE 'RUS' 
				 END
	,rls_group = CONCAT(UPPER((SELECT TOP 1 group_rls FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_tr)),'_',
						CASE 
							WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_tr) = 'TR' THEN 'TUR'
							ELSE 'RUS' 
							END
						)
	,rls_company = CONCAT(UPPER(level_b.name_en),'_',
							CASE 
								WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_tr) = 'TR' THEN 'TUR'
								ELSE 'RUS' 
							END
							)
	,rls_businessarea = CONCAT(UPPER(emp.business_area),'_',
								CASE 
									WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_tr) = 'TR' THEN 'TUR'
									ELSE 'RUS' 
								END
								)
  ,ua.*
FROM {{ source('stg_enc_kpi', 'raw_enocta_kpi_t_fact_useractions') }}  ua
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }} emp ON emp.global_id = ua.USER_CODE
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_a') }} level_a ON level_a.code = emp.a_level_code
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_b') }} level_b ON level_b.code = emp.b_level_code
