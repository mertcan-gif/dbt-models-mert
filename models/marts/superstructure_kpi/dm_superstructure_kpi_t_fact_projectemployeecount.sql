{{
  config(
    materialized = 'table',tags = ['superstructure_kpi']
    )
}}	


SELECT
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT('REC_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(es.business_area,''),'_',COALESCE(kuc.RegionCode,''))
	,es.*
FROM {{ source('stg_sharepoint', 'raw__superstructure_kpi_t_fact_employeesatisfaction') }} es
    LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON kuc.RobiKisaKod = 'REC' 

