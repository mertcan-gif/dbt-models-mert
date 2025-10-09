
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','cashflow']
    )
}}

WITH cf_adjusted4 AS
(
	select         
        *
        ,CAST('' AS NVARCHAR) AS ptext
        ,CAST('' AS NVARCHAR) AS [high_level_breakdown]
		,interest_calculation_date = CAST(document_date AS DATE)
		,interest_period = CONCAT(YEAR(CAST(document_date AS DATE)),RIGHT(LEFT(CAST(document_date AS DATE),7),2))
		,included_in_o_n_interest = 'X'
        ,'NO' as is_adjusting_document
	from {{ ref('stg__nwc_kpi_t_fact_cashflownotadjusted') }}
	UNION ALL 
    select * from {{ ref('stg__nwc_kpi_v_fact_reccashadjustment') }}
)
SELECT

	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(company ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(business_area,''),'_',COALESCE(kuc.RegionCode,''))
	,cf_adjusted4.*
FROM cf_adjusted4
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON cf_adjusted4.company = kuc.RobiKisaKod