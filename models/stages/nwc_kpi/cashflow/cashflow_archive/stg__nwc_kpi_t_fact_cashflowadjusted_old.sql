
{{
  config(
    materialized = 'table',tags = ['nwc_kpi_old','cashflow_old']
    )
}}

WITH cf_adjusted4 AS
(
	select         
        *
        ,CAST('' AS NVARCHAR) AS ptext
        ,CAST('' AS NVARCHAR) AS [ÜST KIRILIM]
		,[Faiz Hesaplanma Tarihi] = CAST(bldat AS DATE)
		,[Faiz Dönemi] = CONCAT(YEAR(CAST(bldat AS DATE)),RIGHT(LEFT(CAST(bldat AS DATE),7),2))
		,[O/N Faiz Hesabına Dahil] = 'X'
        ,'NO' as is_adjusting_document
	from {{ ref('stg__nwc_kpi_t_fact_cashflownotadjusted_old') }}
	UNION ALL 
    select * from {{ ref('stg__nwc_kpi_v_fact_reccashadjustment_old') }}
)
SELECT

	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(RBUKRS ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(rbusa,''),'_',COALESCE(kuc.RegionCode,''))
	,cf_adjusted4.*
	,[BLDAT] AS [BELGE TARİHİ]
FROM cf_adjusted4
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON cf_adjusted4.RBUKRS = kuc.RobiKisaKod