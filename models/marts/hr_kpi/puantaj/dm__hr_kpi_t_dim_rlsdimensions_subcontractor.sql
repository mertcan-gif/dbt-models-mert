{{
  config(
    materialized = 'table',tags = ['hr_kpi_puantaj']
    )
}}
with kyriba_union as (
  SELECT DISTINCT 
    rls_region,
    rls_group,
    rls_company,
    rls_businessarea,
    rls_key, 
    [group],
    company,
    business_area
  FROM {{ ref('dm__hr_kpi_t_fact_subcontractorpersonnellogs_transformed') }}
  WHERE rls_key <>'_--'

  UNION  

  SELECT DISTINCT 
    rls_region,
    rls_group,
    rls_company,
    rls_businessarea,
    rls_key, 
    [group],
    company,
    business_area
  FROM {{ ref('dm__hr_kpi_t_dim_subcontractorpersonnel_transformed') }}
  where rls_key<>'_--'
  ),
  
combination as 
(
  select r.rls_key, p.rls_profile from kyriba_union as r
  left join {{ ref('vw__rls_v_dim_profileentitymapping') }} p on p.rls_region = r.rls_region
where p.rls_profile LIKE 'DWH_L0%'
  and p.rls_profile IS NOT NULL
  and r.rls_region IS NOT NULL

union all 

select r.rls_key, p.rls_profile from kyriba_union as r
  left join {{ ref('vw__rls_v_dim_profileentitymapping') }} p on p.rls_group = r.[rls_group]
where p.rls_profile IS NOT NULL
  and (p.rls_profile like 'DWH_L1%' or p.rls_profile like 'DWH_LS_GR_%')
  and r.rls_region IS NOT NULL

union all

select r.rls_key, p.rls_profile from kyriba_union as r
  left join {{ ref('vw__rls_v_dim_profileentitymapping') }} p on p.rls_company = r.rls_company
where p.rls_profile IS NOT NULL
  and (p.rls_profile like 'DWH_L2%' or p.rls_profile like 'DWH_LS_CO_%')
  and r.rls_region IS NOT NULL


union all

select r.rls_key, p.rls_profile from kyriba_union as r
  left join {{ ref('vw__rls_v_dim_profileentitymapping') }} p on p.rls_businessarea = r.rls_businessarea
where p.rls_profile IS NOT NULL
  and (p.rls_profile like 'DWH_L3%' or p.rls_profile like 'DWH_LS_BA_%')
  and r.rls_region IS NOT NULL

union all 

select 
 distinct 
    r.rls_key,
    rls_profile = 'DWH_L0_GLOBAL'
  from kyriba_union as r
  where 1=1
    and r.rls_region IS NOT NULL
)

select distinct rls_key, rls_profile 
from combination