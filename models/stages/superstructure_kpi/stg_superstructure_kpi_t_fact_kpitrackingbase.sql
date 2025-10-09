{{
  config(
    materialized = 'table',tags = ['superstructure_kpi']
    )
}}	

WITH bu_cte AS (

	SELECT * FROM {{ source('stg_sharepoint', 'raw__superstructure_kpi_t_dim_businessunits') }}
	
	-- SELECT 
	-- 	business_unit = CASE
	-- 						WHEN LEN(functional_description) - LEN('Ust Yapı Business Unit') IS NULL 
	-- 							OR LEN(functional_description) - LEN('Ust Yapı Business Unit') < 0 THEN NULL
	-- 						ELSE CONCAT('BU-',RIGHT(functional_description, LEN(functional_description) - LEN('Ust Yapı Business Unit')))
	-- 					END,
	-- 	business_area = LEFT(rls_businessarea,LEN(rls_businessarea)-4)
	-- FROM {{ ref('vw__rls_v_dim_profileentitymapping') }}
	-- where 1=1
	-- 	and rls_profile like '%USTYA%'
	-- 	and rls_profile <> 'DWH_LS_BA_RECUSTYAPI'
	-- 	and rls_businessarea NOT LIKE '%0000%'
	-- 	and rls_businessarea NOT IN ('R006_TUR','R033_TUR','R036_TUR','R037_TUR','R007_TUR','R032_TUR','R043_TUR','R052_TUR')
	-- 	and rls_businessarea IN (
	-- 					'R065_TUR',
	-- 					'R055_TUR',
	-- 					'R054_TUR',
	-- 					'R010_TUR',
	-- 					'R001_TUR',
	-- 					'R005_TUR',
	-- 					'R040_TUR',
	-- 					'R067_TUR',
	-- 					'R057_TUR',
	-- 					'R058_TUR',
	-- 					'R061_TUR',
	-- 					'R062_TUR',
	-- 					'R068_TUR',
	-- 					'R070_TUR',
	-- 					'R071_TUR'
	-- 					)
),

unionized_data as (

SELECT 
	bu_cte.business_unit,
	cf.business_area,
	bu_cte.business_area_description,
	posting_date = EOMONTH(posting_date),
	amount_in_eur = SUM(amount_in_eur),
	[type] = N'Nakit Akış'
FROM {{ ref('dm__nwc_kpi_t_fact_cashflow') }} cf
	LEFT JOIN bu_cte ON bu_cte.business_area = cf.business_area
WHERE 1=1
	AND business_unit IS NOT NULL
	AND company = 'REC'
	AND [type] <> 'KAR'
	AND [type] <> N'TEMETTÜ'
	AND [type] <> N'FAİZ REEL'
	AND [type] <> N'FAİZ NPV'
	AND [type] <> N'KDV DÜZELTME'
	AND [type] <> N'FAİZ'


GROUP BY 
	bu_cte.business_unit,
	cf.business_area,
	bu_cte.business_area_description,
	EOMONTH(posting_date)

UNION ALL

SELECT 
	bu_cte.business_unit,
	rp.business_area,
	business_area_description = TGSBT.gtext,
	posting_date = EOMONTH(date),
	amount_in_eur = SUM(revenue_poc_target_eur),
	[type] = 'Revenue (PoC) - Beklenti'
FROM {{ source('stg_sharepoint', 'raw__superstructure_kpi_t_fact_revenuepoccost') }} rp
	LEFT JOIN bu_cte ON bu_cte.business_area = rp.business_area
	LEFT JOIN (SELECT * FROM {{ ref('vw__s4hana_v_sap_ug_tgsbt') }} WHERE SPRAS = 'TR' ) TGSBT ON rp.business_area = [TGSBT].GSBER
WHERE 1=1
	AND business_unit IS NOT NULL
	AND company = 'REC'
GROUP BY 
	bu_cte.business_unit,
	rp.business_area,
	TGSBT.gtext,
	EOMONTH(date)

UNION ALL

SELECT 
	bu_cte.business_unit,
	rp.business_area,
	business_area_description = TGSBT.gtext,
	posting_date = EOMONTH(date),
	amount_in_eur = SUM(cost_target_eur),
	[type] = 'Gider - Beklenti'
FROM {{ source('stg_sharepoint', 'raw__superstructure_kpi_t_fact_revenuepoccost') }} rp
	LEFT JOIN bu_cte ON bu_cte.business_area = rp.business_area
	LEFT JOIN (SELECT * FROM {{ ref('vw__s4hana_v_sap_ug_tgsbt') }} WHERE SPRAS = 'TR' ) TGSBT ON rp.business_area = [TGSBT].GSBER
WHERE 1=1
	AND business_unit IS NOT NULL
	AND company = 'REC'
GROUP BY 
	bu_cte.business_unit,
	rp.business_area,
	TGSBT.gtext,
	EOMONTH(date)

UNION ALL

SELECT 
	bu_cte.business_unit,
	cr.business_area,
	bu_cte.business_area_description,
	posting_date = EOMONTH(posting_date),
	amount_in_eur = SUM(amount_in_eur * -1),
	[type] = 'Ciro (Faturalı)'
FROM {{ ref('dm__nwc_kpi_t_fact_costrealization') }} cr
	LEFT JOIN bu_cte ON bu_cte.business_area = cr.business_area
WHERE 1=1
	AND business_unit IS NOT NULL
	AND company = 'REC'
	AND cr.[type] IN (N'GELİR', N'GELIR')
	AND (cr.[fiscal_period] NOT IN ('13','14','15','16') OR cr.[fiscal_period] IS NULL)
GROUP BY 
	bu_cte.business_unit,
	cr.business_area,
	bu_cte.business_area_description,
	EOMONTH(posting_date)

UNION ALL

SELECT 
	bu_cte.business_unit,
	np.business_area,
	business_area_description = TGSBT.gtext,
	posting_date = EOMONTH(np.budat_eomonth),
	amount_in_eur = SUM(cumulative_total_monthly * avg_eur_curr_monthly),
	[type] = 'Ciro (PoC) - Başberi'
FROM {{ ref('stg__nwc_kpi_t_fact_netprofitifrsrevenue') }} np
	LEFT JOIN bu_cte ON bu_cte.business_area = np.business_area
	LEFT JOIN (SELECT * FROM {{ ref('vw__s4hana_v_sap_ug_tgsbt') }} WHERE SPRAS = 'TR' ) TGSBT ON np.business_area = [TGSBT].GSBER
	LEFT JOIN {{ ref('stg__dimensions_t_dim_averages4currencies') }} s4c ON budat_eomonth = s4c.eomonth
								AND np.budget_currency = s4c.currency collate database_default
WHERE 1=1
	AND business_unit IS NOT NULL
	AND company = 'REC'
GROUP BY 
	bu_cte.business_unit,
	np.business_area,
	TGSBT.gtext,
	EOMONTH(np.budat_eomonth)

UNION ALL

{# SELECT 
	bu_cte.business_unit,
	cr.business_area,
	cr.business_area_description,
	posting_date = EOMONTH(posting_date),
	amount_in_eur = SUM(amount_in_eur * -1),
	[type] = 'Gider - Başberi'
FROM {{ ref('dm__nwc_kpi_t_fact_costrealization') }} cr
	LEFT JOIN bu_cte ON bu_cte.business_area = cr.business_area
WHERE 1=1
	AND bu_cte.business_unit IS NOT NULL
	AND cr.company = 'REC'
	AND cr.[type] IN (N'GİDER', N'GIDER')
	AND (cr.fiscal_period NOT IN ('13','14','15','16','00') OR cr.fiscal_period IS NULL)
	AND cr.document_type <> 'IA'
	AND cr.document_type <> 'SA'
GROUP BY 
	bu_cte.business_unit,
	cr.business_area,
	cr.business_area_description,
	EOMONTH(posting_date) #}

SELECT 
	bu_cte.business_unit,
	np.business_area,
	business_area_description = TGSBT.gtext,
	posting_date = EOMONTH(np.budat_eomonth),
	amount_in_eur = SUM(cumulative_total_monthly * avg_eur_curr_monthly),
	[type] = 'Gider - Başberi'
FROM {{ ref('stg__nwc_kpi_t_fact_netprofitcost') }} np
	LEFT JOIN bu_cte ON bu_cte.business_area = np.business_area
	LEFT JOIN (SELECT * FROM {{ ref('vw__s4hana_v_sap_ug_tgsbt') }} WHERE SPRAS = 'TR' ) TGSBT ON np.business_area = [TGSBT].GSBER
	LEFT JOIN {{ ref('stg__dimensions_t_dim_averages4currencies') }} s4c ON budat_eomonth = s4c.eomonth
								AND np.budget_currency = s4c.currency collate database_default
WHERE 1=1
	AND business_unit IS NOT NULL
	AND company = 'REC'
GROUP BY 
	bu_cte.business_unit,
	np.business_area,
	TGSBT.gtext,
	EOMONTH(np.budat_eomonth)

)

SELECT 
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT('REC_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(business_area,''),'_',COALESCE(kuc.RegionCode,''))
	,u.*
FROM unionized_data u
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON kuc.RobiKisaKod = 'REC' 
