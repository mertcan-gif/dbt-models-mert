{{
  config(
    materialized = 'table',tags = ['nwc_kpi','stockaging']
    )
}}

WITH RAW_DATA AS (

	SELECT * FROM {{ ref('stg__nwc_kpi_t_fact_stockaginghistoricalraw') }}

	UNION ALL

	SELECT * FROM {{ ref('stg__nwc_kpi_t_fact_stockagingraw') }}

	UNION ALL

	SELECT * FROM {{ ref('stg__nwc_kpi_t_fact_stockagingexternal') }}


)
SELECT
	[rls_region]   = CASE WHEN company IN ('TKM','ARN') THEN 'EUR' ELSE kuc.RegionCode END
	,[rls_group]   = CASE 
						WHEN company IN ('TKM','ARN') THEN 'RETGROUP_EUR' 
						ELSE CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
					 END
	,[rls_company] = CASE
						WHEN company IN ('TKM','ARN') THEN CONCAT(COALESCE(company ,''),'_EUR','')
						ELSE CONCAT(COALESCE(company ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
					 END
	,[rls_businessarea] = CONCAT(COALESCE(RAW_DATA.business_area,''),'_',COALESCE(kuc.RegionCode,''))
	,RAW_DATA.*
	,kyriba_group = kuc.KyribaGrup
	,kyriba_company_code = kuc.KyribaKisaKod
FROM RAW_DATA
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON RAW_DATA.company = kuc.RobiKisaKod
