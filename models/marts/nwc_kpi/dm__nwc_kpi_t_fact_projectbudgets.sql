
{{
  config(
    materialized = 'table',tags = ['nwc_kpi']
    )
}}

SELECT 
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(p.company ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(p.business_area_code,''),'_',COALESCE(kuc.RegionCode,''))
  ,p.*
	,kuc.KyribaGrup
	,kuc.RobiKisaKod
FROM {{ ref('stg__nwc_kpi_t_fact_projectbudgets') }} p
  LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON p.company = kuc.RobiKisaKod