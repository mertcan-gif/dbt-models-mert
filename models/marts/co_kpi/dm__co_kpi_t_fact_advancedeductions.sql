{{
  config(
    materialized = 'table',tags = ['co_kpi']
    )
}}

WITH rd AS (
SELECT
	rls_region = cm.RegionCode
	,rls_group = CONCAT(cm.KyribaGrup, '_',cm.RegionCode)
	,rls_company = CONCAT(acd.rbukrs, '_',cm.RegionCode)
	,rls_businessarea = CONCAT(acd.rbusa, '_',cm.RegionCode)
	,kyriba_grup = cm.KyribaGrup
	,company = acd.[rbukrs]
	,businessarea = acd.[rbusa]
	,businessarea_description = t001w.name1
	,vendor_code = acd.kunnr
	,vendor = kna1.name1
	,fiscal_year = acd.[gjahr]
	,document_no = acd.[belnr]
	,document_type = acd.[blart]
	,line_item = acd.[buzei]
	,general_ledger_account = acd.[racct]
	,document_date = acd.[bldat]
	,posting_date = acd.[budat]
	,document_currency = acd.[rwcur]
	,amount_in_document_currency = acd.[hsl]
	,description = acd.[sgtxt]
  FROM {{ ref("stg__s4hana_t_sap_acdoca") }} acd
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w on t001w.werks = acd.rbusa
  LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm ON cm.RobiKisaKod = acd.rbukrs
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} kna1 ON kna1.kunnr = acd.kunnr 
  WHERE acd.[racct] LIKE '440%'
		AND acd.[umskz] = '0'
		AND acd.[blart] = 'SV'
	)

SELECT 
	*
	,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM rd
