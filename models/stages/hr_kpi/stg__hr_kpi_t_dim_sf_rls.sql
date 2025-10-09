{{
  config(
    materialized = 'table',tags = ['hr_kpi','dim_rls']
    )
}}


WITH RLS AS 
(
	select
		rls_region = CASE 
						WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_tr) = 'TR' THEN 'TUR'
						ELSE 'NAN' 
					 END
		,rls_group = UPPER((SELECT TOP 1 group_rls FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_tr))
		,rls_company = UPPER(level_b.name_en)
		,rls_businessarea = UPPER(emp.business_area)
		,emp.[user_id] as 'user_id'
		,emp.[sap_id] as 'sap_id'
		,concat(emp.name, ' ', emp.surname) as 'name_surname'
		,level_a.name_tr as 'a_level_name'
		,level_b.name_tr as 'b_level_name'
		,level_c.name_tr as 'c_level_name'
		,level_d.name_tr as 'd_level_name'
		,level_e.name_tr as 'e_level_name'
	from {{ ref('stg__hr_kpi_t_dim_employees_union_raw') }}  as emp 
		LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levela_union') }} level_a ON level_a.code = emp.a_level_code
		LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levelb_union') }} level_b ON level_b.code = emp.b_level_code
		LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levelc_union') }} level_c ON level_c.code = emp.c_level_code
		LEFT JOIN {{ ref('stg__hr_kpi_t_dim_leveld_union') }} level_d ON level_d.code = emp.d_level_code
		LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levele_union') }} level_e ON level_e.code = emp.e_level_code 
	where 1=1
)
 
select 
	rls_region 
	,rls_group = CONCAT(COALESCE([rls_group],''),'_',COALESCE([rls_region],''))
	,rls_company = CONCAT(COALESCE([rls_company],''),'_',COALESCE([rls_region],''))
	,rls_businessarea = CONCAT(COALESCE([rls_businessarea],''),'_',COALESCE([rls_region],''))
	,[user_id]
	,sap_id
	,name_surname
from RLS