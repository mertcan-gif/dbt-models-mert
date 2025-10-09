{{
  config(
    materialized = 'table',tags = ['nwc_kpi','balance_reconciliation']
    )
}}

SELECT 
	[group] = kuc.KyribaGrup,
	[robi_company_code] = kuc.RobiKisaKod,
	company = RBUKRS,
	counterparty_group = CASE WHEN ACDOCA.LIFNR <> '' THEN kuc2.KyribaGrup ELSE kuc3.KyribaGrup END,
	counterparty_company = CASE WHEN ACDOCA.LIFNR <> '' THEN kuc2.RobiKisaKod ELSE kuc3.RobiKisaKod END,
	general_ledger_account = RACCT,
	general_ledger_description = SKAT.txt50,
	vendor_code = ACDOCA.LIFNR,
	customer_code = ACDOCA.KUNNR,
	document_number = ACDOCA.BELNR,
	clearing_document_number = ACDOCA.AUGBL,
	document_line_item = ACDOCA.BUZEI,
	fiscal_year = ACDOCA.GJAHR,
	posting_date = ACDOCA.BUDAT,
	document_date = ACDOCA.BLDAT,
	main_account = LEFT(RACCT,3), -- Borç Türü
	document_currency = RWCUR, --Döviz Cinsi
	company_currency = T001.WAERS,
	amount_in_company_currency = CASE
										WHEN TCURX.CURRDEC = 3 THEN HSL/10 
									ELSE HSL END,
	amount_in_eur = KSL,
	amount_in_document_currency = CASE
										WHEN TCURX.CURRDEC = 3 THEN WSL/10 
									ELSE WSL END
	,item_text = SGTXT
	,kyriba_group = kuc.KyribaGrup
	,kyriba_company_code = kuc.KyribaKisaKod
FROM {{ ref('stg__s4hana_t_sap_acdoca') }} AS ACDOCA
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} LFA1 ON ACDOCA.LIFNR = LFA1.LIFNR
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} KNA1 ON ACDOCA.KUNNR = KNA1.KUNNR
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} T001W ON ACDOCA.RBUSA = T001W.WERKS
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001') }} T001 ON ACDOCA.RBUKRS = T001.BUKRS
	LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} BKPF ON
				ACDOCA.BELNR = BKPF.BELNR 
				AND ACDOCA.RBUKRS = BKPF.BUKRS
				AND ACDOCA.GJAHR = BKPF.GJAHR
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON ACDOCA.RWCUR = TCURX.CURRKEY
	LEFT JOIN {{ ref('vw__s4hana_v_sap_ug_skat') }} SKAT on ACDOCA.racct = SKAT.saknr AND SKAT.spras = 'T'
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON (kuc.RobiKisaKod IS NOT NULL AND ACDOCA.RBUKRS = kuc.RobiKisaKod ) OR (kuc.RobiKisaKod IS NULL AND kuc.KyribaKisaKod=ACDOCA.RBUKRS) 
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc2 ON (kuc2.RobiKisaKod IS NOT NULL AND ACDOCA.LIFNR = kuc2.RobiKisaKod ) OR (kuc2.RobiKisaKod IS NULL AND kuc2.KyribaKisaKod=ACDOCA.LIFNR)  --Kyribakısakod mu bağlanmalı?
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc3 ON (kuc3.RobiKisaKod IS NOT NULL AND ACDOCA.KUNNR = kuc3.RobiKisaKod ) OR (kuc3.RobiKisaKod IS NULL AND kuc3.KyribaKisaKod=ACDOCA.KUNNR) 
	-- LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc4 ON ACDOCA.KUNNR = kuc4.KyribaKisaKod
	-- LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc5 ON ACDOCA.KUNNR = kuc5.KyribaKisaKod

WHERE 1=1
	AND LEFT(RACCT,3) > 99 AND LEFT(RACCT,3) < 530
	AND LEFT(RACCT,3) NOT IN (
						'150'
						,'152'
						,'153'
						,'157'
						,'181'
						,'198'
						--,'226'
						,'250'
						,'252'
						,'253'
						,'254'
						,'255'
						,'267'
						,'379'
						,'381'
						,'393'
					)
	--	AND ACDOCA.AUGBL = ''
	AND (LEN(ACDOCA.LIFNR)=3 OR LEN(ACDOCA.KUNNR)=3)
	AND ACDOCA.BUZEI <> '000'
	AND NETDT <> '00000000'	
	AND ACDOCA.BUDAT <= CAST(EOMONTH(DATEADD(M,-1,GETDATE())) AS DATE)

	--Ön kayıtlar da gelmiyor, uyuşmazlık olursa ön kayıt mı diye kontrol edilebilir.
	-- 379 çıkart, 1 ile 5'le başlayanların üç haneli lifnrlileri alacağız

