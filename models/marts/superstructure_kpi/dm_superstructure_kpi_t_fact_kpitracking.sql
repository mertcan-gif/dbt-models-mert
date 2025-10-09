{{
  config(
    materialized = 'table',tags = ['superstructure_kpi']
    )
}}	


SELECT 
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT('REC_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(business_area,''),'_',COALESCE(kuc.RegionCode,''))
	,company = 'REC'
	,u.*
FROM {{ ref('stg_superstructure_kpi_t_fact_kpitrackingcumulative') }} u
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON kuc.RobiKisaKod = 'REC' 

UNION ALL

SELECT 
	[rls_region]   = kuc.RegionCode,
	[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,'')),
	[rls_company] = CONCAT('REC_'	,COALESCE(kuc.RegionCode,''),''),
	[rls_businessarea] = CONCAT(COALESCE(np.business_area,''),'_',COALESCE(kuc.RegionCode,'')),
	company = 'REC',
	np.business_area,
	business_area_description = TGSBT.gtext,
	posting_date = EOMONTH(np.budat_eomonth),
	[type] = 'Ciro (PoC)',
	cumulative_amount_in_eur = cumulative_total * avg_eur_curr_ytd,
	amount_in_eur = 0
FROM {{ ref('stg__nwc_kpi_t_fact_netprofitifrsrevenue') }} np
	RIGHT JOIN aws_stage.sharepoint.raw__superstructure_kpi_t_dim_businessunits bu ON bu.business_area = np.business_area
	LEFT JOIN (SELECT * FROM {{ ref('vw__s4hana_v_sap_ug_tgsbt') }} WHERE SPRAS = 'TR' ) TGSBT ON np.business_area = [TGSBT].GSBER
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON kuc.RobiKisaKod = 'REC' 
	LEFT JOIN {{ ref('stg__dimensions_t_dim_averages4currencies') }} ac on ac.currency = np.budget_currency
																				and ac.[eomonth] = np.budat_eomonth
WHERE 1=1
	AND company = 'REC'

UNION ALL 

SELECT 
	[rls_region]   = kuc.RegionCode,
	[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,'')),
	[rls_company] = CONCAT('REC_'	,COALESCE(kuc.RegionCode,''),''),
	[rls_businessarea] = CONCAT(COALESCE(np.business_area,''),'_',COALESCE(kuc.RegionCode,'')),
	company = 'REC',
	np.business_area,
	business_area_description = TGSBT.gtext,
	posting_date = EOMONTH(np.budat_eomonth),
	[type] = 'Gider',
	cumulative_amount_in_eur = cumulative_total_eur,
	amount_in_eur = 0
FROM {{ ref('stg__nwc_kpi_t_fact_netprofitcost') }} np
	RIGHT JOIN aws_stage.sharepoint.raw__superstructure_kpi_t_dim_businessunits bu ON bu.business_area = np.business_area
	LEFT JOIN (SELECT * FROM {{ ref('vw__s4hana_v_sap_ug_tgsbt') }} WHERE SPRAS = 'TR' ) TGSBT ON np.business_area = [TGSBT].GSBER
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON kuc.RobiKisaKod = 'REC' 
WHERE 1=1
	AND company = 'REC'

UNION ALL 

SELECT 
	[rls_region]   = kuc.RegionCode,
	[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,'')),
	[rls_company] = CONCAT('REC_'	,COALESCE(kuc.RegionCode,''),''),
	[rls_businessarea] = CONCAT(COALESCE(ir.business_area,''),'_',COALESCE(kuc.RegionCode,'')),
	company = 'REC',
	ir.business_area,
	business_area_description = TGSBT.gtext,
	posting_date = EOMONTH(ir.budat_eomonth),
	[type] = 'Ciro (Faturalı) - YTD',
	cumulative_amount_in_eur = cumulative_total_eur,
	amount_in_eur = 0
FROM {{ ref('stg_superstructure_kpi_t_fact_invoicedrevenueytd') }} ir
	RIGHT JOIN aws_stage.sharepoint.raw__superstructure_kpi_t_dim_businessunits bu ON bu.business_area = ir.business_area
	LEFT JOIN (SELECT * FROM {{ ref('vw__s4hana_v_sap_ug_tgsbt') }} WHERE SPRAS = 'TR' ) TGSBT ON ir.business_area = [TGSBT].GSBER
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON kuc.RobiKisaKod = 'REC' 
	LEFT JOIN {{ ref('stg__dimensions_t_dim_averages4currencies') }} ac on ac.currency = ir.document_currency
																				and ac.[eomonth] = ir.budat_eomonth
