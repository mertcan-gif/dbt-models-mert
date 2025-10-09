{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}

with UNIONIZED_LICENSES as (
  select
    [email_address]
    ,'Power BI' AS license_group
    ,'Power BI Pro' AS license_type
    ,segment
    ,DATEADD(DAY,-1,snapshot_date) snapshot_date
  from {{ ref('stg__powerbi_kpi_t_fact_licenses') }}

  UNION ALL

  select
      user_id,
      license_group,
      license_type,
      segment,
      snapshot_date =  CAST(DATEADD(DAY,-1,GETDATE()) AS DATE)
  from {{ ref('stg__rnet_kpi_t_fact_licenses') }}

  UNION ALL

  select
      email_address,
      license_group,
      license_type,
      segment,
      snapshot_date
  from {{ ref('stg__s4_kpi_t_fact_licenses') }}

  UNION ALL

  select
      email_address,
      license_group,
      license_type,
      segment,
      snapshot_date
  from {{ ref('stg__rmore_kpi_t_fact_licenses') }}
)

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
    ,l.email_address
    ,CONCAT(emp.[name], ' ', emp.surname) AS name_surname
    ,license_group
    ,license_type
    ,segment
    ,snapshot_date
FROM UNIONIZED_LICENSES l
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }} emp ON emp.email_address = l.email_address
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_a') }} level_a ON level_a.code = emp.a_level_code
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_b') }} level_b ON level_b.code = emp.b_level_code
