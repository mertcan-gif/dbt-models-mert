
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','rlsdimensions']
    )
}}

with rls_key_raw as (

			select distinct r.rls_region,r.rls_group,r.rls_company,r.rls_businessarea,p.rls_profile 
      from {{ ref('dm__nwc_kpi_t_dim_rlsdimensions') }} r
				left join {{ ref('vw__rls_v_dim_profileentitymapping') }} p on p.rls_region = r.rls_region
			where p.rls_profile LIKE 'DWH_L0%'
				and p.rls_profile IS NOT NULL
				and r.rls_region IS NOT NULL

				union all

			select distinct r.rls_region,r.rls_group,r.rls_company,r.rls_businessarea,p.rls_profile 
      from {{ ref('dm__nwc_kpi_t_dim_rlsdimensions') }} r
				left join {{ ref('vw__rls_v_dim_profileentitymapping') }} p on p.rls_group = r.rls_group
			where p.rls_profile IS NOT NULL
        and (p.rls_profile like 'DWH_L1%' or p.rls_profile like 'DWH_LS_GR_%')
			  and r.rls_region IS NOT NULL

				union all

			select distinct r.rls_region,r.rls_group,r.rls_company,r.rls_businessarea,p.rls_profile 
      from {{ ref('dm__nwc_kpi_t_dim_rlsdimensions') }} r
				left join {{ ref('vw__rls_v_dim_profileentitymapping') }} p on p.rls_company = r.rls_company
			where p.rls_profile IS NOT NULL
        and (p.rls_profile like 'DWH_L2%' or p.rls_profile like 'DWH_LS_CO_%')
        and r.rls_region IS NOT NULL
),

  rls_key_ba_company_union as (
  select distinct 
    rls_key = CONCAT(rls_businessarea,'-',rls_company,'-',rls_group),rls_profile
  from rls_key_raw

  union all

  select distinct 
    rls_key = CONCAT('_',rls_region,'-',rls_company,'-',rls_group),rls_profile
  from rls_key_raw

  union all

  select distinct 
    rls_key = CONCAT(r.rls_businessarea,'-',r.rls_company,'-',r.rls_group),rls_profile
  from {{ ref('dm__nwc_kpi_t_dim_rlsdimensions') }} r
    left join {{ ref('vw__rls_v_dim_profileentitymapping') }} p on p.rls_businessarea = r.rls_businessarea
  where p.rls_profile IS NOT NULL
    and (p.rls_profile like 'DWH_L3%' or p.rls_profile like 'DWH_LS_BA_%')
    and r.rls_region IS NOT NULL

  union all

  select distinct 
    rls_key = CONCAT(r.rls_businessarea,'-',r.rls_company,'-',r.rls_group),rls_profile = 'DWH_L0_GLOBAL'
  from {{ ref('dm__nwc_kpi_t_dim_rlsdimensions') }} r
  where 1=1
    and r.rls_region IS NOT NULL

)

select distinct
  rls_key, rls_profile
from rls_key_ba_company_union