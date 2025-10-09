
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','arap']
    )
}}

WITH RAW_DATA_TurnoverRatio AS (
	SELECT 
		ACDOCA.RBUKRS, --Şirket Kodu
		ACDOCA.RBUSA,
		KUNNR = CASE 
					WHEN KNA1.KUNNR LIKE 'HR%' THEN REPLACE(KNA1.KUNNR,'HR','') 
					ELSE KNA1.KUNNR END ,
		KNA1.NAME1,
		SUM(HSL) AS HSL,
		SUM(CASE WHEN ACDOCA.BLART = 'UE' THEN HSL ELSE 0 END) AS HSL_UE
 	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} AS ACDOCA
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} AS KNA1 ON ACDOCA.KUNNR = KNA1.KUNNR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} AS T001W ON ACDOCA.RBUSA = T001W.WERKS 
		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} AS BKPF ON
					ACDOCA.BELNR = BKPF.BELNR 
					AND ACDOCA.RBUKRS = BKPF.BUKRS
					AND ACDOCA.GJAHR = BKPF.GJAHR
	WHERE 1=1
		AND LEFT(RACCT,3) IN ('120')
		--AND ACDOCA.BLART = 'UE'
		--AND CAST(H_BUDAT AS date) <= '2023-05-31'
		AND LEN(ACDOCA.KUNNR)<>3
		AND BKPF.STBLG <> 'X'
		AND ACDOCA.KUNNR <> ''
		AND ACDOCA.AUGBL = ''
		--AND ACDOCA.RBUKRS = 'REC'
	GROUP BY 
		ACDOCA.RBUKRS, 
		ACDOCA.RBUSA,
		KNA1.KUNNR,
		KNA1.NAME1
	HAVING SUM(HSL) <> 0
)

	SELECT 
		RBUKRS
		,RBUSA
		,KUNNR
		,NAME1
		,HSL
		,HSL_UE
		,TURNOVER_RATIO = CASE
							 WHEN HSL_UE <> 0 AND HSL_UE + HSL <> 0 THEN HSL /((HSL+HSL_UE)/2) 
							 ELSE NULL
						 END
	FROM RAW_DATA_TurnoverRatio
	WHERE KUNNR IS NOT NULL