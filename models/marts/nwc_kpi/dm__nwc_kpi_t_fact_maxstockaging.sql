
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','stockaging','maxstockaging']
    )
}}

SELECT 
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE([company] ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(s.[business_area],''),'_',COALESCE(kuc.RegionCode,''))
	,s.*
	,kyriba_group = kuc.KyribaGrup
	,kyriba_company_code = kuc.RobiKisaKod
FROM {{ ref('stg__nwc_kpi_t_fact_maxstockaging') }} s 
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON s.[company] = kuc.RobiKisaKod


