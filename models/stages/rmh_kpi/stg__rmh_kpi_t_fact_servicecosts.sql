{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
SELECT 
	[rls_region] = cm.RegionCode
	,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
	,rls_company = acd.rbukrs + '_' + cm.RegionCode
	,[rls_businessarea] = acd.rbusa + '_' + cm.RegionCode
	,cm.KyribaGrup AS [group]
	,acd.rbukrs AS company
	,acd.rbusa AS business_area
	/**
	Iletilen mapping tablosundaki hesaplar için lifnr ve kunnr bos geliyor. 
	Mertcan Beyden bilgi alınarak gkont kolonunda vendor kodları bulunduğu belirlenmiştir.
	**/
	--,acd.lifnr AS vendor_code   
	--,acd.kunnr AS customer_code
	,acd.gkont AS vendor_code
	,lfa.name1 AS vendor_name
	,acd.gjahr AS fiscal_year
	,acd.belnr AS document_no
	,acd.buzei AS document_line_item
	,acd.racct AS general_ledger_account
	,cast(acd.bldat AS date) AS document_date
	,acd.ksl AS eur_value
	,acd.osl AS usd_value
	,acd.hsl AS try_value
	,acm.expense_category
	,acm.expense_sub_category
FROM {{ source('stg_rmh_kpi', 'raw__rmh_kpi_t_dim_accountmapping') }} acm
LEFT JOIN {{ ref('stg__s4hana_t_sap_acdoca') }} acd on acm.saknr = acd.racct
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa on lfa.lifnr = acd.gkont
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm on acd.rbukrs = cm.RobiKisaKod
LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf ON acd.rbukrs = bkpf.bukrs
									AND acd.belnr = bkpf.belnr
									AND acd.gjahr = bkpf.gjahr
WHERE 1=1
	--idari işler ekibinden gelen cari listesi filtrelenmiştir.
	--AND acd.gkont IN (select saknr from rmh_kpi.raw__rmh_kpi_t_dim_accountmapping acm WHERE account_id = 1)
	AND bkpf.xreversing = 0
	AND bkpf.xreversed = 0  
	-- Müge Sevinç Hanım ile örnek verilere bakarak şirket filtresi ile maliyetlerin tuttuğu görüldüğü için eklenmiştir.
	AND acd.rbukrs = 'RMH' 