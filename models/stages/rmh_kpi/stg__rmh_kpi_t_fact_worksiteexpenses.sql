{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}

SELECT
	rls_region = cm.RegionCode
	,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
	,rls_company = acd.rbukrs + '_' + cm.RegionCode
	,rls_businessarea = acd.rbusa + '_' + cm.RegionCode
	,cm.KyribaGrup AS [group]
	,acd.rbukrs AS company
	,acd.gkont AS current_code
	,lfa1.name1 AS current_name
	,acd.rbusa AS business_area
	,acd.gjahr AS fiscal_year
	,cast(acd.bldat AS date) AS document_date
	,acd.matnr AS material_code
	,makt.maktx AS material_description
	,office_name = t001w.name1
	,acd.belnr AS document_no
	,acd.buzei AS document_line_item
	,acd.racct AS general_ledger_account
	,REVERSE(SUBSTRING(REVERSE(txt50), 1, CHARINDEX('-', REVERSE(txt50)) - 1)) AS category
	,contract_number = NULL
	,contract_eur_value = NULL
	,contract_usd_value = NULL
	,contract_try_value = NULL
	,acd.ksl AS eur_value
	,acd.osl AS usd_value
	,acd.hsl AS try_value
FROM {{ ref('stg__s4hana_t_sap_acdoca') }} acd  
LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf ON bkpf.belnr = acd.belnr 
												AND bkpf.gjahr = acd.gjahr 
												AND acd.rbukrs = bkpf.bukrs 
LEFT JOIN (SELECT * FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_skat') }} WHERE ktopl='RONS' AND spras = 'T') ska ON acd.racct = ska.saknr 
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON acd.rbukrs = cm.RobiKisaKod
LEFT JOIN {{ ref('dm__dimensions_t_dim_dailys4currencies') }} ds on CAST(acd.bldat AS date) = ds.date_value
																AND ds.currency = acd.rwcur
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa1 ON lfa1.lifnr = acd.gkont
LEFT JOIN (SELECT * FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_makt') }} WHERE spras = 'T') makt ON RIGHT(makt.matnr, '8') = acd.matnr 
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON t001w.werks = acd.rbusa
WHERE acd.matnr IN (
					'40008510'
					,'40008509'
					)
	AND bkpf.xreversing = 0
	AND bkpf.xreversed = 0  
	AND racct LIKE '7%'
	AND acd.blart <> 'CD'