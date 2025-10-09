
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','advanceaging','advanceagingwithcleareditems']
    )
}}

SELECT 
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(RBUKRS,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(rbusa,''),'_',COALESCE(kuc.RegionCode,''))
	,company = CASE
					WHEN RBUKRS = 'PLB' THEN 'NS_PLB'
					WHEN RBUKRS = 'RAC' THEN 'NS_RAC'
					WHEN RBUKRS = 'RCZ' THEN 'NS_RCR12'
				ELSE RBUKRS end
	,business_area = RBUSA
	,document_number = BELNR
	,document_type = BLART
	,document_date = BLDAT
	,posting_date = BUDAT
	,main_account = LEFT(RACCT,3)
	,account_type = CASE
						WHEN LEFT(RACCT,3) = '159' THEN 'VERİLEN SİPARİŞ AVANSLARI'
						WHEN LEFT(RACCT,3) = '179' THEN 'TAŞERONLARA VERİLEN AVANSLAR'
						WHEN LEFT(RACCT,3) = '195' THEN 'İŞ AVANSLARI'
						WHEN LEFT(RACCT,3) = '196' THEN 'PERSONEL AVANSLARI'
						WHEN LEFT(RACCT,3) = '259' THEN 'VERİLEN AVANSLAR'
						WHEN LEFT(RACCT,3) = '340' THEN 'ALINAN SİPARİŞ AVANSLARI'
						WHEN LEFT(RACCT,3) = '440' THEN 'ALINAN SİPARİŞ AVANSLARI'
					END
	,account_group = CASE
						WHEN LEFT(RACCT,3) = '159' THEN 'VERİLEN AVANSLAR'
						WHEN LEFT(RACCT,3) = '179' THEN 'VERİLEN AVANSLAR'
						WHEN LEFT(RACCT,3) = '195' THEN 'VERİLEN AVANSLAR'
						WHEN LEFT(RACCT,3) = '196' THEN 'VERİLEN AVANSLAR'
						WHEN LEFT(RACCT,3) = '259' THEN 'VERİLEN AVANSLAR'
						WHEN LEFT(RACCT,3) = '340' THEN 'ALINAN AVANSLAR'
						WHEN LEFT(RACCT,3) = '440' THEN 'ALINAN AVANSLAR'
					end
	,vendor_code = CASE 
						WHEN LFA1.LIFNR <> '' THEN LFA1.LIFNR
						ELSE KNA1.KUNNR 
					END
	,vendor = CASE
				WHEN LFA1.LIFNR <> '' THEN LFA1.NAME1
				ELSE KNA1.NAME1
			END
	,business_area_description = T001W.NAME1
	,document_currency = RWCUR
	,amount_in_document_currency = CASE
										WHEN RWCUR IN ('IQD','LYD','MZM','BHD') THEN WSL/10
									ELSE WSL END  --İşlem para birimi
	,kyriba_group = kuc.KyribaGrup
	,kyriba_company_code = kuc.KyribaKisaKod
	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} AS acdoca
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa1  ON acdoca.LIFNR = lfa1.LIFNR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} kna1  ON acdoca.KUNNR = kna1.KUNNR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON acdoca.RBUSA = t001w.WERKS
		LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON acdoca.RBUKRS = kuc.RobiKisaKod
	WHERE 1=1
		AND acdoca.blart IS NOT NULL
		-- AND acdoca.augbl = ''
		AND LEFT(RACCT,3) IN (
			'159'
			,'179'
			,'195'
			,'196'
			,'259'
			,'340'
			,'440'
			)
		AND (
			ACDOCA.LIFNR <> '' 
			OR ACDOCA.KUNNR <> '' 
			OR (ACDOCA.LIFNR <> '' AND ACDOCA.KUNNR <> '')
		)