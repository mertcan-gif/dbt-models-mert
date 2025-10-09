{{
  config(
    materialized = 'table',tags = ['nwc_kpi','duetoduefrom']
    )
}}
	

--	SELECT 
--		company = ACDOCA.RBUKRS,
--		business_area = ACDOCA.RBUSA, --ACDOCA=> RBUSA
--		posting_date = CASE WHEN CAST(ACDOCA.BUDAT AS date) > '2022-12-31' THEN CAST(ACDOCA.BUDAT AS date)
--							ELSE '2022-12-31' END,--ACDOCA=> BUDAT
--		document_currency = ACDOCA.RWCUR,
--		budget_currency = bv.pb,
--		amount = CASE
--					WHEN bv.pb = 'EUR' THEN ACDOCA.KSL
--					WHEN bv.pb = 'USD' THEN ACDOCA.OSL
--					ELSE ACDOCA.HSL
--				END,
--		amount_eur = ACDOCA.KSL,
--		amount_usd = ACDOCA.OSL,
--		amount_hsl = ACDOCA.HSL ,
--		[type] = CASE 
--				WHEN RACCT = '6000201002' THEN 'HURDA'
--				WHEN LEFT(RACCT,3) = '600' THEN 'GELIR'
--				WHEN ((RBUKRS = 'REC' AND LEFT(RCNTR, 4) = 'RECX') OR (ACDOCA.RBUKRS = 'RMI' AND LEFT(RCNTR, 4) = 'RMIX')) THEN 'KAR'
--				WHEN LEFT(RACCT,3) = '770' THEN 'GYG'
--				ELSE 'GIDER'
--			END
--		,pb = bv.pb
--		,FISCYEARPER
--	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} ACDOCA
--		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} BKPF ON (ACDOCA.GJAHR = BKPF.GJAHR)
--			AND (ACDOCA.BELNR = BKPF.BELNR)
--			AND (ACDOCA.RBUKRS = BKPF.BUKRS)
--		RIGHT JOIN {{ ref('stg__nwc_kpi_t_dim_duetoduefrombudgetcurrencies') }} bv ON bv.gsber = ACDOCA.rbusa
--		
--	WHERE 1=1
--		AND rbukrs IN ('REC','RMI')
--		AND 
--			( Left(RACCT, 3) = '740' OR Left(RACCT, 3) = '770' OR
--				( RBUKRS IN ('REC','RMI','HCA','RIA')
--				  AND RACCT IN ('6000102001','6000202001','6000201001','6000201002') 
--				  AND LEFT(GKONT,3) <> '350')
--			  )
--		AND (ACDOCA.BLART = 'WA' OR ([BKPF].XREVERSING = 0 AND [BKPF].XREVERSED = 0))		
--		AND ACDOCA.BLART <> 'SA'
--		
--		/** RET Verilerinin sadece excel'den gönderediklerinin gözükmesi istendi **/
--		AND ACDOCA.RBUKRS <> 'RET'
--		AND LEFT(RIGHT(FISCYEARPER,6),2) NOT IN ('13','14','15','16') OR ACDOCA.FISCYEARPER IS NULL
--		AND ACDOCA.BLART <> 'IA'

SELECT 
	company,
	business_area, --ACDOCA=> RBUSA
	posting_date = CASE WHEN CAST(posting_date AS date) > '2022-12-31' THEN CAST(posting_date AS date)
						ELSE '2022-12-31' END,
	document_currency,
	budget_currency = bv.pb,
	amount = CASE
				WHEN bv.pb = 'EUR' THEN amount_in_eur 
				WHEN bv.pb = 'USD' THEN amount_in_usd
				ELSE amount_in_tl
			END,
	amount_eur = amount_in_eur,
	amount_usd = amount_in_usd,
	amount_hsl = amount_in_tl ,
	[type]
	,pb = bv.pb
	,[period]
FROM {{ ref('dm__nwc_kpi_t_fact_costrealization') }} t
	RIGHT JOIN {{ ref('stg__nwc_kpi_t_dim_duetoduefrombudgetcurrencies') }} bv ON bv.gsber = t.business_area
WHERE 1=1
	AND company IN ('REC','RMI')
	AND ([fiscal_period] NOT IN ('13','14','15','16','00') OR [fiscal_period] IS NULL)
