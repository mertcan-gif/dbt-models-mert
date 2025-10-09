{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

WITH RAW_CTE AS (
	SELECT
		company = ACDOCA.RBUKRS, --Şirket Kodu
		document_number = ACDOCA.BELNR,
		document_line_item = ACDOCA.BUZEI,
		fiscal_year = ACDOCA.GJAHR,
		document_type = ACDOCA.BLART,
		posting_date = ACDOCA.BUDAT,
		document_date = ACDOCA.BLDAT,
		entry_date = BKPF.CPUDT,
		main_account = LEFT(RACCT,3), -- Alacak Türü
		customer_code = CASE 
					WHEN KNA1.KUNNR LIKE 'HR%' THEN REPLACE(KNA1.KUNNR,'HR','')
					ELSE KNA1.KUNNR END, -- Müşteri Kodu
		customer = KNA1.NAME1,
		business_area = ACDOCA.RBUSA, --İş Alanı
		business_area_description = T001W.NAME1, -- Proje Adı
		document_currency = RWCUR, --Döviz Cinsi
		--SHKZG,
		amount_in_document_currency = CASE
				  WHEN TCURX.CURRDEC = 3 THEN WSL/10 
			  ELSE WSL END,
		amount_in_company_currency = HSL,
		due_date = CASE WHEN NETDT <> 00000000 THEN CAST(NETDT AS DATE) ELSE NULL END,
		due_days = DATEDIFF(DAY,CAST(GETDATE() AS DATE),
					CASE WHEN NETDT <> 00000000 THEN CAST(NETDT AS DATE) ELSE NULL END)
 	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} AS ACDOCA
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} AS KNA1 ON ACDOCA.KUNNR = KNA1.KUNNR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} AS T001W ON ACDOCA.RBUSA = T001W.WERKS
		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} AS BKPF ON
					ACDOCA.BELNR = BKPF.BELNR 
					AND ACDOCA.RBUKRS = BKPF.BUKRS
					AND ACDOCA.GJAHR = BKPF.GJAHR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} AS TCURX ON ACDOCA.RWCUR = TCURX.CURRKEY
		LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON ACDOCA.RBUKRS = kuc.RobiKisaKod
	WHERE 1=1
		AND kuc.KyribaGrup = N'RGYGROUP'
		AND LEFT(RACCT,3) IN ('120', '128')
		AND LEN(ACDOCA.KUNNR)<>3
		AND BKPF.STBLG <> 'X'
		AND ACDOCA.KUNNR <> ''
		-- AND ACDOCA.AUGBL = ''
)

SELECT
	[rls_region]   = kuc.RegionCode
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(RAW_CTE.company  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(RAW_CTE.business_area  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,RAW_CTE.*
	,CASE WHEN due_days<0 THEN 'Overdue' ELSE 'Outstanding' END AS receivable_category
	,CASE
		WHEN ABS(due_days) <=30 THEN '0-30 Days'
		WHEN ABS(due_days) <=60 THEN '31-60 Days'
		WHEN ABS(due_days) <=90 THEN '61-90 Days'
		WHEN ABS(due_days) <=180 THEN '91-180 Days'
		WHEN ABS(due_days) <=365 THEN '181-365 Days'
		ELSE '>365 Days'
	END as due_category
	,CASE WHEN nodel = '1' THEN 'Passive' ELSE 'Active' END AS active_status
FROM RAW_CTE
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} kuc ON RAW_CTE.company = kuc.RobiKisaKod
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_knb1') }} knb1 ON RAW_CTE.[company] = knb1.bukrs
														AND RAW_CTE.customer_code = knb1.kunnr
	WHERE customer_code BETWEEN '1000000' AND '1999999'
