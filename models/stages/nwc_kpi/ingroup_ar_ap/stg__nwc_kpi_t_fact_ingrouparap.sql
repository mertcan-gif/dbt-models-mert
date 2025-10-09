
{{
  config(
    materialized = 'table',tags = ['ingroup_arap_hourly']
    )
}}




SELECT 
	company = ACDOCA.RBUKRS, --Şirket Kodu
	document_number = ACDOCA.BELNR,
	document_line_item = ACDOCA.BUZEI,
	fiscal_year = ACDOCA.GJAHR,
	document_type = ACDOCA.BLART,
	posting_date = ACDOCA.BUDAT,
	document_date = ACDOCA.BLDAT,
	general_ledger = LEFT(RACCT,3), -- Borç Türü
	general_ledger_account = RACCT,
	general_ledger_account_text = skat2.txt50,
	offsetting_account_number = GKONT,
	offsetting_account_text = skat.txt50,
	item_text = SGTXT,
	customer_vendor_code = CASE WHEN ACDOCA.LIFNR <> '' THEN ACDOCA.LIFNR ELSE ACDOCA.KUNNR END,
	customer_vendor_name = CASE WHEN ACDOCA.LIFNR <> '' THEN LFA1.NAME1 ELSE KNA1.NAME1 END,
	business_area = ACDOCA.RBUSA, --İş Alanı
	business_area_description = T001W.NAME1, -- Proje Adı
	document_currency = RWCUR, --Döviz Cinsi
	company_currency = T001.WAERS,
	amount_in_document_currency = CASE
				WHEN TCURX.CURRDEC = 3 THEN cast(WSL as money)/10 
			ELSE cast(WSL as money) END,
	amount_in_company_currency = cast(HSL as money),
	/**
		Emrah Mustafa Arıcan Bey'in isteği üzerine NETDT'si boş olan kayıtlar için due date kayıt tarihi + 15 olarakyansıtılıyor.
	**/
	due_date = CASE WHEN NETDT = '' THEN DATEADD(D,15,CAST(BUDAT AS DATE)) ELSE CAST(NETDT AS DATE) END,
	due_days = CASE WHEN NETDT = '' THEN DATEDIFF(DAY,CAST(GETDATE() AS DATE),DATEADD(D,15,CAST(BUDAT AS DATE))) ELSE DATEDIFF(DAY,CAST(GETDATE() AS DATE),CAST(NETDT AS DATE)) END,
	/**
		Aktif Pasif projelerin filtrelenmesi için aşağıdaki kolon eklenmiştir.
		Aktif Pasif bilgisi RTI'nın bütçelerinden alındığı için, RTI'ya dahil olmayan
		tüm projeler için 'ACTIVE' değeri atanmıştır.
	**/
	is_active = CASE
					WHEN RBUSA IN (SELECT DISTINCT gsber  FROM [aws_stage].[sharepoint].raw__nwc_kpi_t_dim_rtibudgets) THEN 'ACTIVE'
					WHEN kuc.KyribaGrup LIKE '%RTI%' AND RBUSA NOT IN (SELECT DISTINCT gsber  FROM [aws_stage].[sharepoint].raw__nwc_kpi_t_dim_rtibudgets) THEN 'PASSIVE'
					ELSE 'ACTIVE'
				END,
	account_type = CASE 
						WHEN (LEN(ACDOCA.KUNNR) = 3 OR LEN(ACDOCA.LIFNR) = 3) THEN 'In-Group'
						ELSE 'Out-Group'
					END

FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_acdoca_ingrouparap') }} ACDOCA
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} LFA1 ON ACDOCA.LIFNR = LFA1.LIFNR
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} KNA1 ON ACDOCA.KUNNR = KNA1.KUNNR
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} T001W ON ACDOCA.RBUSA = T001W.WERKS
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001') }} T001 ON ACDOCA.RBUKRS = T001.BUKRS
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON ACDOCA.RWCUR = TCURX.CURRKEY
	LEFT JOIN {{ ref('vw__s4hana_v_sap_ug_skat') }} SKAT on ACDOCA.gkont = SKAT.saknr AND SKAT.spras = 'T'
	LEFT JOIN {{ ref('vw__s4hana_v_sap_ug_skat') }} SKAT2 on ACDOCA.racct = SKAT2.saknr AND SKAT2.spras = 'T'
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON (kuc.RobiKisaKod IS NOT NULL AND ACDOCA.RBUKRS = kuc.RobiKisaKod ) OR (kuc.RobiKisaKod IS NULL AND kuc.KyribaKisaKod=ACDOCA.RBUKRS) 
WHERE 1=1
	AND LEFT(RACCT,1) < '6'
	AND ACDOCA.AUGBL = ''
	and ACDOCA.koart IN ('K','D')
	-- and ACDOCA.rbukrs = 'RMG'
	-- and (ACDOCA.KUNNR = 'REC' OR ACDOCA.LIFNR = 'REC')
	-- AND (LEN(ACDOCA.KUNNR) = 3 OR LEN(ACDOCA.LIFNR) = 3)
