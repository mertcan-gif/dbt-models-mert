{{
  config(
    materialized = 'view',tags = ['nwc_kpi_draft','credits_draft']
    )
}}
	
WITH RAW_CTE AS
(
	SELECT 
		company = ACDOCA.RBUKRS, --Şirket Kodu
		document_number = ACDOCA.BELNR,
		document_line_item = ACDOCA.BUZEI,
		fiscal_year = ACDOCA.GJAHR,
		document_type = ACDOCA.BLART,
		posting_date = ACDOCA.BUDAT,
		document_date = ACDOCA.BLDAT,
		entry_date = BKPF.CPUDT,
		main_account = LEFT(RACCT,3), -- Borç Türü
		vendor_code = LFA1.LIFNR, -- Satıcı Kodu
		vendor = LFA1.NAME1, -- Satıcı Adı
		business_area = ACDOCA.RBUSA, --İş Alanı
		business_area_description = T001W.NAME1, -- Proje Adı
		document_currency = RWCUR, --Döviz Cinsi
		amount_in_document_currency = CASE
				  WHEN TCURX.CURRDEC = 3 THEN WSL/10 
			  ELSE WSL END,
		amount_in_company_currency = HSL

	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} AS ACDOCA
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} LFA1 ON ACDOCA.LIFNR = LFA1.LIFNR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} T001W ON ACDOCA.RBUSA = T001W.WERKS 
		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} BKPF ON
					ACDOCA.BELNR = BKPF.BELNR 
					AND ACDOCA.RBUKRS = BKPF.BUKRS
					AND ACDOCA.GJAHR = BKPF.GJAHR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON ACDOCA.RWCUR = TCURX.CURRKEY
	WHERE 1=1
		AND LEFT(RACCT,3) = '300'
		and (LEFT(RIGHT(ACDOCA.fiscyearper,6),2) not in ('13','14','15','16','00') OR LEFT(RIGHT(ACDOCA.fiscyearper,6),2) is null)
		--AND CAST(H_BUDAT AS date) <= '2023-05-31'
		--AND LEN(ACDOCA.LIFNR)<>3
		--AND _BKPF.STBLG <> 'X'
		--AND ACDOCA.BUZEI <> '000'
		--AND ACDOCA.LIFNR <> ''
		--AND ACDOCA.AUGBL = ''
		--AND NETDT <> '00000000'

)
SELECT
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(RAW_CTE.company ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(RAW_CTE.business_area ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,RAW_CTE.*
	,kyriba_group = kuc.KyribaGrup
	,kyriba_company_code = kuc.KyribaKisaKod
FROM RAW_CTE
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON RAW_CTE.company = kuc.RobiKisaKod

