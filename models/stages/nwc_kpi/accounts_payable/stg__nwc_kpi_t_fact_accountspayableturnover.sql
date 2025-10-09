
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','arap']
    )
}}

WITH RAW_DATA_TURNOVER AS (
	SELECT 
		ACDOCA.RBUKRS, --Şirket Kodu
		ACDOCA.RBUSA,
		LFA1.LIFNR,
		LFA1.NAME1,
		SUM(HSL) AS HSL,
		SUM(CASE WHEN ACDOCA.BLART = 'UE' THEN HSL ELSE 0 END) AS HSL_UE
 	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} AS ACDOCA
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} AS LFA1 ON ACDOCA.LIFNR = LFA1.LIFNR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} AS T001W ON ACDOCA.RBUSA = T001W.WERKS 
		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} AS BKPF ON
					ACDOCA.BELNR = BKPF.BELNR 
					AND ACDOCA.RBUKRS = BKPF.BUKRS
					AND ACDOCA.GJAHR = BKPF.GJAHR
	WHERE 1=1
		AND LEFT(RACCT,3) IN ('320')
		--AND ACDOCA.BLART = 'UE'
		--AND CAST(H_BUDAT AS date) <= '2023-05-31'
		AND LEN(ACDOCA.LIFNR)<>3
		AND BKPF.STBLG <> 'X'
		AND ACDOCA.BUZEI <> '000'
		AND ACDOCA.LIFNR <> ''
		AND ACDOCA.AUGBL = ''
		AND NETDT <> '00000000'
	GROUP BY 
		ACDOCA.RBUKRS, --Şirket Kodu
		ACDOCA.RBUSA,
		LFA1.LIFNR,
		LFA1.NAME1	
	HAVING SUM(HSL) <> 0
)
	SELECT 
		RBUKRS
		,RBUSA
		,LIFNR
		,NAME1
		,HSL
		,HSL_UE
		,TURNOVER_RATIO = CASE
							 WHEN HSL_UE <> 0 AND HSL_UE + HSL <> 0 THEN HSL /((HSL+HSL_UE)/2) 
							 ELSE NULL
						 END
	FROM RAW_DATA_TURNOVER
	WHERE LIFNR IS NOT NULL

