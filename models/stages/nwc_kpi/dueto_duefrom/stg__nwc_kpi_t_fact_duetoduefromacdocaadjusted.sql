{{
  config(
    materialized = 'table',tags = ['nwc_kpi','duetoduefrom']
    )
}}


WITH ACDOCA_GIDER AS 
(
	SELECT 
		company,
		RBUSA = business_area,
		PostingDate = posting_date,
		document_currency,
		budget_currency,
		HSL = amount_hsl, 
		KSL = amount_eur,
		OSL = amount_usd,
		[type]

	FROM {{ ref('stg__nwc_kpi_t_fact_duetoduefromacdocaraw') }}
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON document_currency = TCURX.CURRKEY
		WHERE [type] = N'MERKEZ' or [type] = N'GIDER' or [type] = N'GİDER'
),


KUMULATIF_MALIYET_CTE AS (

	SELECT 
		* 
		,AnaHesap = 'kümülatif_maliyet'
	FROM ACDOCA_GIDER

	-- UNION ALL

	-- SELECT
	-- 	rti_a.bukrs
	-- 	,rti_a.[gsber]
	-- 	,[h_budat] = CASE WHEN CAST([h_budat] AS date) > '2022-12-31' THEN CAST([h_budat] AS date)
	-- 					ELSE '2022-12-31' END
	-- 	,[h_waers]
	-- 	,budget_currency = bv.pb
	-- 	,rti_a.balance_tl
	-- 	,rti_a.balance_euro
	-- 	,rti_a.balance_dollar
	-- 	,[type] = CASE 
	-- 					WHEN [type] = N'MERKEZ' THEN 'GIDER' ELSE [type] 
	-- 				END
	-- 	,AnaHesap = 'kümülatif_maliyet'
	-- FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_rtiaccrualadjustment') }} rti_a
	-- 		RIGHT JOIN {{ ref('stg__nwc_kpi_t_dim_duetoduefrombudgetcurrencies') }} bv ON rti_a.gsber = bv.gsber 
	-- WHERE [type] = N'MERKEZ' or [type] = N'GIDER' or [type] = N'GİDER' 
),

KUMULATIF_GELIR_CTE AS 
(
	SELECT 
		company,
		RBUSA = business_area,
		PostingDate = posting_date,
		budget_currency,
		HSL = amount_hsl, 
		KSL = amount_eur,
		OSL = amount_usd,
		[type],
		AnaHesap = 'kümülatif_gelir'
	FROM {{ ref('stg__nwc_kpi_t_fact_duetoduefromacdocaraw') }}
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON document_currency = TCURX.CURRKEY
		WHERE [type] = N'GELIR' OR [type] = N'GELİR'

--	UNION ALL
--
--		SELECT
--			rti_a.bukrs 	
--			,rti_a.[gsber] 
--			,[h_budat] = CAST([h_budat] AS DATE)
--			,budget_currency = bv.pb
--			,rti_a.balance_tl
--			,rti_a.balance_euro
--			,rti_a.balance_dollar
--			,[type] 
--			,AnaHesap = 'kümülatif_gelir'
--	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_rtiaccrualadjustment') }} rti_a
--		RIGHT JOIN {{ ref('stg__nwc_kpi_t_dim_duetoduefrombudgetcurrencies') }} bv ON rti_a.gsber = bv.gsber
--		WHERE [type] = N'GELIR' OR [type] = N'GELİR'

)

SELECT
	company
	,RBUSA
	,EOMONTH(PostingDate) AS PostingDate
	,budget_currency
	,HSL = SUM(HSL)
	,KSL = SUM(KSL)
	,OSL = SUM(OSL)
	,[type]
	,AnaHesap
FROM KUMULATIF_MALIYET_CTE
GROUP BY
	company
	,RBUSA
	,EOMONTH(PostingDate)
	,budget_currency
	,[type]
	,AnaHesap			

UNION ALL

SELECT
	company
	,RBUSA
	,EOMONTH(PostingDate) AS PostingDate
	,budget_currency
	,SUM(HSL)
	,SUM(KSL)
	,SUM(OSL)
	,[type]
	,AnaHesap 
FROM KUMULATIF_GELIR_CTE
GROUP BY
	company
	,RBUSA
	,EOMONTH(PostingDate)
	,budget_currency
	,[type]
	,AnaHesap	
