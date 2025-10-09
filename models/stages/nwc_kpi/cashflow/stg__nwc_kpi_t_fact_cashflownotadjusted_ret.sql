{{
  config(
    materialized = 'table',tags = ['nwc_kpi','cashflow_ret']
    )
}}

WITH cf_base AS 
(
	SELECT
		acdoca.gjahr,
		acdoca.racct,
		acdoca.rbusa,
		budat = CAST(acdoca.budat AS DATE),
		acdoca.belnr,
		acdoca.rbukrs,
		bldat = CAST(acdoca.bldat AS DATE),
		acdoca.buzei,
		acdoca.blart,
		acdoca.gkont,
		acdoca.sgtxt,
		bkpf.cpudt,
		bkpf.xreversing,
		bkpf.xreversed,
		bkpf.stblg,
		bkpf.usnam,
		Left(RACCT, 3) AS general_ledger,
		wsl AS [amount_in_document_currency],
		acdoca.rwcur,
		acdoca.rtcur,
		hsl as [amount_try],
		osl AS [amount_usd],
		ksl AS [amount_eur],
		tsl AS [amount_teblig],
		Left(acdoca.belnr, 2) AS belgn,
		IIf(LFA1.lifnr <> '',LFA1.lifnr,IIf(KNA1.kunnr <> '', KNA1.kunnr, '')) AS account_code,
		IIf(LFA1.name1 <> '', LFA1.name1, IIf(KNA1.name1 <> '', KNA1.name1, '')) AS account_name
	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} AS acdoca
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} kna1 ON acdoca.kunnr = kna1.kunnr
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa1 ON acdoca.lifnr = lfa1.lifnr
			LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf ON acdoca.rbukrs = bkpf.bukrs
									AND acdoca.belnr = bkpf.belnr
									AND acdoca.gjahr = bkpf.gjahr
	WHERE 1=1
			AND acdoca.rbukrs IN (SELECT DISTINCT company FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retcashaccrualmapping') }})
			AND acdoca.rbusa IN (SELECT DISTINCT business_area FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retcashaccrualmapping') }})
			AND LEFT(acdoca.racct,3) = '102'
			AND buzei <> '000'
			AND acdoca.blart IN ('S1','S2','S3')
			-- AND	bkpf.xreversing = 0
			-- AND bkpf.xreversed = 0		 
			-- AND LEFT(acdoca.racct, 3) <> '900'	
			-- AND LEFT(acdoca.racct, 3) <> '901' 
			-- AND LEFT(acdoca.racct, 3) <> '899'
/*8*/		AND (acdoca.GKONT <> '1029999999' OR (acdoca.RBUSA = 'H067' AND acdoca.GKONT = '1029999999' AND acdoca.RACCT = '1029999999'))
	
) 
,cf_adjusted1 AS (
	SELECT *,
			IIf(
			[financial_item] <> '',
			[financial_item],
			IIf(Left(RACCT, 3) = '102', '102000', [financial_item])
		) AS [mali kalem2]
	FROM cf_base		
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_mainaccounts') }} a_hesaplar ON cf_base.RACCT = a_hesaplar.main_account

)
,[cf_adjusted2] AS
(
	SELECT
		fiscal_year = cf_adjusted1.gjahr,
		general_ledger_account = cf_adjusted1.racct,
		business_area = cf_adjusted1.rbusa,
		business_area_description = tgsbt.gtext,
		posting_date = cf_adjusted1.budat,
		document_number = cf_adjusted1.belnr,
		company = cf_adjusted1.rbukrs,
		document_date = cf_adjusted1.bldat,
		entry_date = cf_adjusted1.cpudt,
		document_line_item = cf_adjusted1.buzei,
		document_type = cf_adjusted1.blart,
		offsetting_account_number = cf_adjusted1.gkont,
		item_text = cf_adjusted1.sgtxt,
		cf_adjusted1.general_ledger,
		[amount_in_document_currency] = 
                    CASE
						  WHEN tcurx.currdec = 3 THEN cf_adjusted1.[amount_in_document_currency]/10 
					  ELSE cf_adjusted1.[amount_in_document_currency] END,
		document_currency = cf_adjusted1.rwcur,
		amount_transaction_currency = cf_adjusted1.rtcur,
		amount_in_tl = cf_adjusted1.[amount_try],
		amount_in_usd = cf_adjusted1.[amount_usd],
		amount_in_eur = cf_adjusted1.[amount_eur],
		amount_notification = cf_adjusted1.[amount_teblig],
		category = CASE 
						WHEN cf_adjusted1.blart ='S3' AND LEFT([cf_adjusted1].belnr,2)='37' AND [cf_adjusted1].rbukrs='RET' AND [cf_adjusted1].rbusa IN ('H067', 'H068') THEN 'FONLAMA'
						WHEN [cf_adjusted1].rbukrs = 'RET' AND [cf_adjusted1].rbukrs IN ('H068','H067') AND cf_adjusted1.[financial_item] = '642100' THEN 'FAİZ'
						WHEN yeka.[business_area] IS NOT NULL THEN UPPER(yeka.[type])
						WHEN cf_adjusted1.blart = 'S2' AND LEFT(gkont,3) = '642' THEN 'FINANS'
						WHEN cf_adjusted1.blart = 'S2' AND LEFT(gkont,3) = '646' THEN 'FINANS'
						WHEN cf_adjusted1.blart = 'S2' AND LEFT(gkont,3) = '659' AND amount_try < 0 THEN 'FINANS'
						WHEN cf_adjusted1.blart = 'S2' AND cf_adjusted1.gkont IN (SELECT DISTINCT customer_vendor_code FROM aws_stage.sharepoint.raw__nwc_kpi_t_fact_retcashaccrualmapping rca2) THEN 'GELİR'
						WHEN cf_adjusted1.blart = 'S1' AND LEFT(gkont,3) = '656' THEN 'FINANS'
						WHEN cf_adjusted1.blart = 'S1' AND LEFT(gkont,3) = '193' THEN 'FINANS'
						WHEN cf_adjusted1.blart = 'S1' AND LEFT(gkont,3) = '659' AND amount_try > 0 THEN 'FINANS'
						WHEN cf_adjusted1.blart = 'S3' AND LEFT(gkont,3) = '102' THEN 'MERKEZ'
						-- WHEN LEFT(racct,1) = '7' AND LEFT(gkont,3) = '770' THEN 'MERKEZ' -- Bu tahakkuktan gelecek
						WHEN cf_adjusted1.blart IN ('S1','S2') THEN 'GIDER' 
					ELSE NULL END,
		cf_adjusted1.[financial_item] as commitment_item,
		[group] = CASE 
						WHEN cf_adjusted1.blart = 'S2' AND LEFT(gkont,3) = '642' THEN N'3) FİNANS' --'FAİZ GELİRLERİ'
						WHEN cf_adjusted1.blart = 'S2' AND LEFT(gkont,3) = '646' THEN N'3) FİNANS' --'KUR FARKI GELİRLERİ'
						WHEN cf_adjusted1.blart = 'S2' AND LEFT(gkont,3) = '659' AND amount_try < 0 THEN N'3) FİNANS' --HEDGE
						WHEN cf_adjusted1.blart = 'S2' AND cf_adjusted1.gkont IN (SELECT DISTINCT customer_vendor_code FROM aws_stage.sharepoint.raw__nwc_kpi_t_fact_retcashaccrualmapping rca2) THEN '1) PROJE GELİRLERİ'
						WHEN cf_adjusted1.blart = 'S1' AND LEFT(gkont,3) = '656' THEN N'3) FİNANS' --'KUR FARKI GİDERLERİ'
						WHEN cf_adjusted1.blart = 'S1' AND LEFT(gkont,3) = '193' THEN N'3) FİNANS' --'STOPAJ GİDERLERİ'
						WHEN cf_adjusted1.blart = 'S1' AND LEFT(gkont,3) = '659' AND amount_try > 0 THEN N'3) FİNANS' --HEDGE
						WHEN cf_adjusted1.blart = 'S3' AND LEFT(gkont,3) = '102' THEN N'4) MERKEZ'
						WHEN cf_adjusted1.blart = 'S3' AND LEFT(gkont,3) = '102' THEN N'4) MERKEZ'
						-- WHEN LEFT(racct,1) = '7' AND LEFT(gkont,3) = '770' THEN '6) ADINA YAPILAN' -- Bu tahakkuktan gelecek
						WHEN cf_adjusted1.blart IN ('S1','S2') THEN N'2) PROJE GİDERLERİ' 
					ELSE NULL END,
		[first_definition] = fi.description_1,
		[definition] = fi.description_2,
		account_code,
		account_name,
		xreversing,
		xreversed,
		reverse_document_number = stblg,
		username = usnam
	FROM cf_adjusted1
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tgsbt') }} tgsbt ON cf_adjusted1.RBUSA = tgsbt.GSBER
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_financialitems') }} fi 
                                                                            ON cf_adjusted1.[mali kalem2] = fi.financial_item
			                                                                AND cf_adjusted1.belgn = fi.document_1
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} tcurx ON cf_adjusted1.rwcur = tcurx.CURRKEY
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_yekavendorlist') }} yeka
			ON [cf_adjusted1].account_code = yeka.[s4_customer_code]
			AND [cf_adjusted1].rbukrs = yeka.company
			AND [cf_adjusted1].rbusa = yeka.business_area
	WHERE 1=1
			AND tgsbt.spras = 'TR'
)		

,stage_cf AS (
SELECT 
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(cf.company  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(cf.business_area,''),'_',COALESCE(kuc.RegionCode,''))
	,cf.*  
	,[type] = category
				-- CASE WHEN rb.main_account IS NOT NULL AND document_type = 'S1' THEN category
				-- 	WHEN rb.main_account IS NULL AND document_type = 'S1' THEN 'MERKEZ'
				-- 	ELSE category
				-- END
	,is_adjusting_document = 'NO'
	,business_area_concatted = CONCAT(cf.business_area,' - ',business_area_description)
	-- ,bank_flag = CASE WHEN rb.main_account IS NOT NULL AND document_type = 'S1' THEN '1' ELSE '0' END
FROM cf_adjusted2 cf
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON cf.company = kuc.RobiKisaKod 
	{# LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_retbanklist') }} rb ON rb.main_account = cf.general_ledger_account
																					AND rb.company = cf.company
																					AND rb.business_area = cf.business_area #}
)

SELECT * FROM stage_cf

UNION ALL

SELECT 
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,fiscal_year
	,general_ledger_account
	,cf.business_area
	,business_area_description
	,posting_date
	,document_number
	,cf.company
	,document_date
	,entry_date
	,document_line_item
	,document_type 
	,offsetting_account_number
	,item_text
	,general_ledger
	,[amount_in_document_currency] = [amount_in_document_currency] * -1
	,document_currency
	,amount_transaction_currency
	,amount_in_tl = amount_in_tl * -1
	,amount_in_usd = amount_in_usd * -1
	,amount_in_eur = amount_in_eur * -1
	,amount_notification = amount_notification * -1
	,category
	,commitment_item
	,[group] = 	'4) MERKEZ' 
	,[first_definition] 
	,[definition] 
	,account_code 
	,account_name 
	,xreversing 
	,xreversed 
	,reverse_document_number 
	,username 
	,[type] = 'GIDER'
	,is_adjusting_document = 'NO'
	,business_area_concatted
	-- ,bank_flag
FROM stage_cf cf
	LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_retbanklist') }} rb ON rb.main_account = cf.general_ledger_account
																					AND rb.company = cf.company
																					AND rb.business_area = cf.business_area
WHERE 1=1
	-- AND bank_flag = '0' AND document_type = 'S1'
	AND rb.main_account IS NULL
	AND document_type = 'S1'

UNION ALL

SELECT 
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(ra.company  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(ra.business_area,''),'_',COALESCE(kuc.RegionCode,''))
	,fiscal_year
	,general_ledger_account
	,business_area
	,business_area_description = tgsbt.gtext
	,posting_date
	,document_number
	,company
	,document_date
	,entry_date
	,document_line_item
	,document_type
	,cast(offsetting_account_number as nvarchar)
	,item_text
	,general_ledger = Left(general_ledger_account, 3)
	,[amount_in_document_currency] = amount_in_bp
	,document_currency
	,amount_transaction_currency = document_currency
	,amount_in_tl
	,amount_in_usd
	,amount_in_eur
	,amount_notification = ''
	,category
	,commitment_item = financial_item
	,[group]
	,[first_definition]
	,[definition]
	,account_code
	,account_name
	,xreversing = ''
	,xreversed = ''
	,reverse_document_number = ''
	,username = ''
	,[type]
	,is_adjusting_document = 'YES'
	,business_area_concatted = CONCAT(ra.business_area,' - ',tgsbt.gtext)
FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retcashadjustments') }} ra
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON ra.company = kuc.RobiKisaKod
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tgsbt') }} tgsbt ON ra.business_area = tgsbt.GSBER

-- UNION ALL

-- SELECT 
-- 		rls_region
-- 		,rls_group
-- 		,rls_company
-- 		,rls_businessarea
-- 		,fiscal_year = ''
-- 		,general_ledger_account = ''
-- 		,business_area
-- 		,business_area_description
-- 		,posting_date
-- 		,document_number = ''
-- 		,company
-- 		,document_date
-- 		,entry_date
-- 		,document_line_item = ''
-- 		,document_type = ''
-- 		,offsetting_account_number = ''
-- 		,item_text = ''
-- 		,general_ledger = ''
-- 		,[amount_in_document_currency]
-- 		,document_currency
-- 		,amount_transaction_currency
-- 		,amount_in_tl
-- 		,amount_in_usd
-- 		,amount_in_eur
-- 		,amount_notification
-- 		,category
-- 		,commitment_item = ''
-- 		,[group] = N'3) PROJE TOPLAMI'
-- 		,[first_definition] = ''
-- 		,[definition] = ''
-- 		,account_code = ''
-- 		,account_name = ''
-- 		,xreversing = ''
-- 		,xreversed = ''
-- 		,reverse_document_number = ''
-- 		,username = ''
-- 		,[type]
-- 		,business_area_concatted
-- FROM stage_cf
-- WHERE 1=1
-- 	AND [group] IN (N'1) PROJE GELIRLERI',N'2) PROJE GIDERLERI')