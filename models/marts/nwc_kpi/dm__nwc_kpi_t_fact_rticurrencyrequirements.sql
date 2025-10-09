
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','cashflow']
    )
}}

SELECT 	
  [rls_region] = (select top 1 RegionCode from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE cr.company = kuc.RobiKisaKod )
	,[rls_group] = 
		CONCAT(
			COALESCE((select top 1 KyribaGrup from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE cr.company = kuc.RobiKisaKod ),'')
			,'_'
			,COALESCE((select top 1 RegionCode from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE cr.company = kuc.RobiKisaKod ),'')
			)
	,[rls_company] =
		CONCAT(
			COALESCE(company,'')
			,'_'
			,COALESCE((select top 1 RegionCode from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE cr.company = kuc.RobiKisaKod ),'')
			)
	,[rls_businessarea] = CONCAT(cr.business_area,'_',(select top 1 RegionCode from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE cr.company = kuc.RobiKisaKod ))
  ,cr.*
  ,[group] = kuc.KyribaGrup

FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_rticurrencyrequirements') }} cr
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON cr.company = kuc.RobiKisaKod 
WHERE 1=1
	AND (kuc.Durum <> 'Pasif' OR kuc.Durum IS NULL)
