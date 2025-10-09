{{
  config(
    materialized = 'table',tags = ['fi_kpi']
    )
}}

/* 
2024-03-10 ANK: RACCT'si 112 ile baslayanlar ML3 sirketi icin banka iliskileri ekibinin istegi uzerine eklenmistir. 
*/

WITH raw_data_cte as (	
	SELECT RBUKRS
		,RACCT
		,RTCUR
		,CAST(TSL AS money) AS Amount
		,CONVERT(DATE, BLDAT, 104) AS _DATE
	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} acdoca WITH (NOLOCK)
		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf ON acdoca.RBUKRS = bkpf.BUKRS
					AND acdoca.BELNR = bkpf.BELNR
					AND acdoca.GJAHR = bkpf.GJAHR
	WHERE 1=1
		AND acdoca.RRCTY = '0'
		AND acdoca.BSTAT<>'C'
		AND BKPF.STBLG = ''
		AND RACCT <> '1029999999' AND (RACCT like '102%'  OR RACCT like '111%'  OR RACCT like '118%' OR RACCT like '112%') 
	)
select 
	*
from raw_data_cte