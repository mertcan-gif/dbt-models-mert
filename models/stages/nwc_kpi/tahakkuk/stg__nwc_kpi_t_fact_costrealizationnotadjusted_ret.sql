
{{
  config(
    materialized = 'table',tags = ['nwc_kpi_draft','tahakkuk_ret']
    )
}}

WITH raw_cr AS (
	SELECT 
		company = ACDOCA.RBUKRS,--ACDOCA=> RBUKRS
		fiscal_year = ACDOCA.GJAHR,
		document_number = ACDOCA.BELNR,
		document_line_item = ACDOCA.BUZEI,
		general_ledger_account = ACDOCA.RACCT, --ACDOCA=> RACCT
		offsetting_account_number = ACDOCA.GKONT,
		business_area = ACDOCA.RBUSA, --ACDOCA=> RBUSA
		posting_date = CAST(ACDOCA.BUDAT AS date),--ACDOCA=> BUDAT
		document_date = CAST(ACDOCA.BLDAT AS date), --ACDOCA=> BLDAT
		entry_date = CAST(BKPF.CPUDT AS DATE),
		day = Right(ACDOCA.[BUDAT], 2),--
		month = RIGHT(LEFT(ACDOCA.[BUDAT], 6),2),--
		year = Left(ACDOCA.[BUDAT], 4),--
		period = Left(ACDOCA.[BUDAT], 4) + RIGHT(LEFT(ACDOCA.[BUDAT], 6),2),--
		amount_in_tl = CAST(ACDOCA.HSL AS MONEY), --IIf([SHKZG] = 'S', [DMBTR], [DMBTR] * -1), --ACDOCA=> --BSEG.HSL AS [BAKİYE TL],
		amount_in_usd = CAST(ACDOCA.OSL AS MONEY), --IIf([SHKZG] = 'S', [DMBE3], [DMBE3] * -1), --ACDOCA=> --BSEG.HSL AS [BAKİYE TL],
		amount_in_eur = CAST(ACDOCA.KSL AS MONEY), --IIf([SHKZG] = 'S', [DMBE2], [DMBE2] * -1), --ACDOCA=> --BSEG.HSL AS [BAKİYE TL],
		amount_in_bp = CAST(ACDOCA.WSL AS MONEY), --IIf([SHKZG] = 'S', WRBTR, WRBTR * -1),
		commitment_item=ACDOCA.FIPEX,
		-- fistl,
		cost_center=ACDOCA.RCNTR,
		cost_center_description = CSKT.KTEXT,
		document_currency = ACDOCA.RWCUR,
		item_text = ACDOCA.SGTXT,
		document_type = ACDOCA.BLART,
		seviye_mapping.level_1,
		seviye_mapping.level_2,
		seviye_mapping.level_3,
		seviye_mapping.level_4,
		level_1_definition =  CASE 
									WHEN LEFT(ACDOCA.RACCT,3) = '600' AND ACDOCA.GKONT IN (SELECT DISTINCT customer_vendor_code FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retcashaccrualmapping') }} rca2) THEN 'SÖZLEŞME GELİRLERİ'
									WHEN LEFT(ACDOCA.RACCT,3) = '642' AND LEFT(ACDOCA.GKONT,3) = '102' AND LEN(ACDOCA.GKONT) = '10' THEN 'FAİZ GELİRLERİ'
									WHEN LEFT(ACDOCA.RACCT,3) = '646' AND (ACDOCA.GKONT IN (SELECT DISTINCT customer_vendor_code FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retcashaccrualmapping') }} rca2) OR (LEFT(ACDOCA.GKONT,3) = '102' AND LEN(ACDOCA.GKONT) = '10')) THEN 'KUR FARKI GELİRLERİ'
									WHEN LEFT(ACDOCA.RACCT,3) = '656' AND (ACDOCA.GKONT IN (SELECT DISTINCT customer_vendor_code FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retcashaccrualmapping') }} rca2) OR (LEFT(ACDOCA.GKONT,3) = '102' AND LEN(ACDOCA.GKONT) = '10')) THEN 'KUR FARKI GİDERLERİ'
									WHEN LEFT(ACDOCA.RACCT,3) = '649' THEN 'HEDGE'
									WHEN LEFT(ACDOCA.RACCT,1) = '6' THEN 'DİĞER GELİRLER'
									WHEN LEFT(ACDOCA.RACCT,1) = '7' AND LEFT(ACDOCA.GKONT,3) = '193' THEN 'STOPAJ GİDERLERİ'
									WHEN LEFT(ACDOCA.RACCT,1) = '7' THEN 'PROJE GİDERLERİ'
								ELSE level1.[description] END,		
		level_2_definition = level2.[description],
		level_3_definition = level3.[description],
		level_4_definition = level4.[description],
		vendor = LFA1.NAME1,
		--customer = KNA1.NAME1,
		business_area_description = [TGSBT].GTEXT,
		[type] = CASE 
						WHEN (RBUKRS = 'RET' AND RBUSA IN ('H067','H068') AND (LEFT(RACCT,3) = '600' OR (UMSKZ = 'A' and ACDOCA.KUNNR = '1028772'))) THEN 'GELIR' -- Semih Doru Bey'den önceden gelen koşul
						WHEN LEFT(ACDOCA.RACCT,3) = '600' AND ACDOCA.GKONT IN (SELECT DISTINCT customer_vendor_code FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retcashaccrualmapping') }} rca2) THEN 'GELIR'
						WHEN LEFT(ACDOCA.RACCT,3) = '642' AND ACDOCA.GKONT = '102' THEN 'FINANS'
						WHEN LEFT(ACDOCA.RACCT,3) = '646' AND (ACDOCA.GKONT IN (SELECT DISTINCT customer_vendor_code FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retcashaccrualmapping') }} rca2) OR (LEFT(ACDOCA.GKONT,3) = '102' AND LEN(ACDOCA.GKONT) = '10')) THEN 'FINANS'
						WHEN LEFT(ACDOCA.RACCT,3) = '656' AND (ACDOCA.GKONT IN (SELECT DISTINCT customer_vendor_code FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retcashaccrualmapping') }} rca2) OR (LEFT(ACDOCA.GKONT,3) = '102' AND LEN(ACDOCA.GKONT) = '10')) THEN 'FINANS'
						WHEN LEFT(ACDOCA.RACCT,3) = '649' THEN 'FINANS'
						WHEN LEFT(ACDOCA.RACCT,1) = '6' THEN 'GIDER' -- Diğer gelirler dedik burada gider göstermek uygun mu
						WHEN LEFT(ACDOCA.RACCT,1) = '7' AND LEFT(ACDOCA.GKONT,3) = '193' THEN 'FINANS'
						WHEN LEFT(ACDOCA.RACCT,1) = '7' THEN 'GIDER'
					ELSE NULL END,

		awkey = BKPF.awkey,
		[pyp_element] = CAST(COALESCE(ACDOCA.psposid,'') AS NVARCHAR)
		,[fiscal_period] = LEFT(RIGHT(FISCYEARPER,6),2)
		,is_adjusting_document = 'NO'
		,vendor_code = LFA1.LIFNR

		,document_header_text = BKPF.bktxt
		,functional_area = ACDOCA.rfarea
		,functional_area_text = BSEG.fkberlong
		,funds_center = ACDOCA.fistl
		,material_number = ACDOCA.MATNR
		,material_description = m.maktx
		,contract_number = e.konnr
		,sas_short_text = e.txz01
		,purchasing_document = ACDOCA.ebeln
		,sas_amount = cast(e.menge as money)
		,unit = e.meins
		,net_price = cast(e.netpr as money)
		,warehouse_document_number = e.ebeln
		,warehouse_amount = cast(bseg.menge as money)
		,warehouse_unit = bseg.meins
		,warehouse_material_description = m2.maktx

	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} ACDOCA
		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} BKPF ON (ACDOCA.GJAHR = BKPF.GJAHR )
			AND (ACDOCA.BELNR = BKPF.BELNR )
			AND (ACDOCA.RBUKRS = BKPF.BUKRS )
		LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_levelmapping') }} seviye_mapping ON ACDOCA.FIPEX = seviye_mapping.level_4 
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} LFA1 ON ACDOCA.GKONT = LFA1.LIFNR 
		LEFT JOIN (SELECT * FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tgsbt') }} WHERE SPRAS = 'TR' ) TGSBT ON ACDOCA.RBUSA = [TGSBT].GSBER
		LEFT JOIN (SELECT * FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_cskt') }} WHERE SPRAS = 'TR' AND KOKRS = 'RONS') CSKT  ON ACDOCA.RCNTR = [CSKT].KOSTL
		LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_leveldescriptions') }} level1 ON seviye_mapping.level_1 = level1.code and level1.level_of_category = 'level_1'
		LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_leveldescriptions') }} level2 ON seviye_mapping.level_2 = level2.code and level2.level_of_category = 'level_2'
		LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_leveldescriptions') }} level3 ON seviye_mapping.level_3 = level3.code and level3.level_of_category = 'level_3'
		LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_leveldescriptions') }} level4 ON seviye_mapping.level_4 = level4.code and level4.level_of_category = 'level_4'
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_bseg') }} BSEG on ACDOCA.rbukrs = BSEG.bukrs and ACDOCA.belnr = BSEG.belnr and ACDOCA.budat = BSEG.hbudat and BSEG.buzei = ACDOCA.buzei
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekpo') }} e ON ACDOCA.matnr=e.matnr and e.bukrs = ACDOCA.rbukrs AND e.ebelp = ACDOCA.ebelp and e.ebeln = ACDOCA.ebeln
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_makt') }} m ON ACDOCA.matnr = RIGHT(m.matnr,8) AND m.spras = 'T' 
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_makt') }}  m2 ON BSEG.matnr = RIGHT(m2.matnr,8) AND m2.spras = 'T' 
	WHERE 1=1 
		AND ACDOCA.rbukrs IN (SELECT DISTINCT company FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retcashaccrualmapping') }})
		AND ACDOCA.rbusa IN (SELECT DISTINCT business_area FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retcashaccrualmapping') }})
		AND	( Left(RACCT, 3) IN ('600','642','646','656','649') OR LEFT(RACCT,1) IN ('6,7') )
		AND (ACDOCA.BLART = 'WA' OR ([BKPF].XREVERSING = 0 AND [BKPF].XREVERSED = 0))		
		AND ACDOCA.BLART <> 'SA'
		AND ACDOCA.BLART <> 'IA'
)

SELECT 
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(company  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(business_area,''),'_',COALESCE(kuc.RegionCode,''))
	,raw_cr.*
	,business_area_concatted = CONCAT(business_area,' - ',business_area_description)
FROM raw_cr
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON raw_cr.company = kuc.RobiKisaKod 