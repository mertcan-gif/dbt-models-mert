{{
  config(
    materialized = 'table',tags = ['fi_kpi','s4mevduat']
    )
}}

SELECT DISTINCT
		NONSAP
		,RBUKRS
		,RACCT
		,TXT20
		,TXT50
		,RTCUR
		,ULKE
		,GRUPORANI
		,GRUP
		,SEKTOR
		,[ALTSEKTOR]
		,[SERBEST]
		,[ULKE_BANKA]
		,[BANKATANIMI]
		,[HESAP_TIPI_TANIMI]
		,[KREDIGRUBU]
		,[CONTRIBUTEGROUP]
		,[YK_SEKTOR]
		,[YK_ULKE]
		,[YK_KREDIGRUBU]
		,[KREDIKATEGORISI]
		,[YK_KREDIKISITI]
		,[KA_KREDIGRUBU]
		,[KA_KREDIKISITI]
		,[CASH_GRUP1]
		,[CASH_GRUP2]
		,[CASH_GRUP3]
		,[CASH_GRUP4] 
	FROM {{ ref('stg__fi_kpi_t_fact_s4mevduat') }}
	WHERE DATE IS NOT NULL