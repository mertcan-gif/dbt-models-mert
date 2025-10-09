{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}

with UNIONIZED_AUTH as (

  select 
    email_address,
	UPPER(display_name COLLATE SQL_Latin1_General_CP1_CI_AS) AS display_name,
    report_id,
	report_name,
    report_user_access_right,
    reporting_date,
    segment
  from {{ ref('stg__powerbi_kpi_t_fact_authorizations') }}

  UNION ALL

  select 
      [user_id],
	  UPPER(full_name COLLATE SQL_Latin1_General_CP1_CI_AS) AS display_name,
      [report_id],
	  report_name, 
      report_user_access_right, 
      GETDATE() as reporting_date,
      segment
  from {{ ref('stg__rnet_kpi_t_fact_authorizations') }}

)

select
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
    ,l.email_address
	,display_name AS name_surname
    ,report_id
	,report_name
    ,report_user_access_right
    ,reporting_date
    ,segment
from UNIONIZED_AUTH l
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }} emp ON emp.email_address = l.email_address
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_a') }} level_a ON level_a.code = emp.a_level_code
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_b') }} level_b ON level_b.code = emp.b_level_code