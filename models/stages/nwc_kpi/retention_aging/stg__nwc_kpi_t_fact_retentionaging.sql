{{
  config(
    materialized = 'table',tags = ['nwc_kpi','retentionaging']
    )
}}
	

SELECT
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(RBUKRS ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(rbusa,''),'_',COALESCE(kuc.RegionCode,''))
	,company = CASE
					WHEN RBUKRS = 'PLB' THEN 'NS_PLB'
					WHEN RBUKRS = 'RAC' THEN 'NS_RAC'
					WHEN RBUKRS = 'RCZ' THEN 'NS_RCR12'
				ELSE RBUKRS END
	,document_number = BELNR
	,document_type = BLART
	,document_date = BLDAT
	,main_account = LEFT(RACCT,3)
	,account_type = CASE 
						WHEN LEFT(RACCT,3) = '126' THEN 'VERİLEN DEPOZİTO VE TEMİNATLAR' --'VERİLEN SİPARİŞ AVANSLARI'
						WHEN LEFT(RACCT,3) = '226' THEN 'VERİLEN DEPOZİTO VE TEMİNATLAR' --'TAŞERONLARA VERİLEN AVANSLAR'
						WHEN LEFT(RACCT,3) = '326' THEN 'ALINAN DEPOZİTO VE TEMİNATLAR'
						WHEN LEFT(RACCT,3) = '426' THEN 'ALINAN DEPOZİTO VE TEMİNATLAR'
					END
	,vendor_code = CASE 
						WHEN LFA1.LIFNR <> '' THEN LFA1.LIFNR
						ELSE KNA1.KUNNR 
					END
	,vendor = CASE
				WHEN LFA1.LIFNR <> '' THEN LFA1.NAME1
				ELSE KNA1.NAME1
			END
	,business_area = RBUSA
	,business_area_description = T001W.NAME1
	,document_currency = RWCUR
	,amount_in_document_currency = CASE
										WHEN TCURX.CURRDEC = 3 THEN WSL/10 
									ELSE WSL END --İşlem para birimi
	,date_diff = DATEDIFF(day,CAST(BLDAT AS date),GETDATE())
	,due_category = CASE
						WHEN ABS(DATEDIFF(day,CAST(BLDAT AS date),GETDATE())) <=30 THEN '0-30 Days'
						WHEN ABS(DATEDIFF(day,CAST(BLDAT AS date),GETDATE())) <=60 THEN '31-60 Days'
						WHEN ABS(DATEDIFF(day,CAST(BLDAT AS date),GETDATE())) <=90 THEN '61-90 Days'
						WHEN ABS(DATEDIFF(day,CAST(BLDAT AS date),GETDATE())) <=180 THEN '91-180 Days'
						WHEN ABS(DATEDIFF(day,CAST(BLDAT AS date),GETDATE())) <=365 THEN '181-365 Days'
						ELSE '>365 Days'
					END
	,kyriba_group = kuc.KyribaGrup
	,kyriba_company_code = kuc.KyribaKisaKod
FROM {{ ref('stg__s4hana_t_sap_acdoca') }} ACDOCA WITH(NOLOCK)
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} LFA1 ON ACDOCA.LIFNR = LFA1.LIFNR
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} KNA1 ON ACDOCA.KUNNR = KNA1.KUNNR
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} T001W ON ACDOCA.RBUSA = T001W.WERKS
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON ACDOCA.RWCUR = TCURX.CURRKEY
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON acdoca.RBUKRS = kuc.RobiKisaKod
WHERE 1=1
	AND LEFT(RACCT,3) IN (
		'126'
		,'226'
		,'326'
		,'426'
		)
	AND ACDOCA.AUGBL = ''
	AND (
			ACDOCA.LIFNR <> '' 
			OR ACDOCA.KUNNR <> '' 
			OR (ACDOCA.LIFNR <> '' AND ACDOCA.KUNNR <> '')
	)






