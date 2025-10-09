{{
  config(
    materialized = 'table',tags = ['fi_kpi']
    )
}}


SELECT 'S' AS NONSAP
	,sfb.RBUKRS
	,sfb.RACCT
	,'Açıklama' as TXT20 
	,'Açıklama' as TXT50 
	,sfb.RTCUR
	,sfb.Amount
	,SUM(Amount) OVER (
		PARTITION BY RACCT
		,RBUKRS
		,RTCUR ORDER BY _DATE
		) AS RunningAmountTotal
	,sfb._DATE AS [DATE]
	,static_comp.ULKE
	,static_comp.GRUPORANI
	,static_comp.GRUP
	,static_comp.SEKTOR
	,static_comp.ALTSEKTOR
	,static_comp.SERBEST
	,(select top 1 is_foreign from {{ source('stg_fi_kpi', 'raw__fi_kpi_t_dim_banks') }} bc WHERE bc.bank_code = SUBSTRING(RACCT,5,3))    AS ULKE_BANKA 
	,(select top 1 bank_name from {{ source('stg_fi_kpi', 'raw__fi_kpi_t_dim_banks') }} bc WHERE bc.bank_code = SUBSTRING(RACCT,5,3))       AS BANKATANIMI 
	,CASE 
		  WHEN RACCT = '1022001014'  THEN 'KKM'

		ELSE
		CASE 
			WHEN SUBSTRING(RACCT,4,1) IS NULL
				THEN 'Tanımsız'
			WHEN SUBSTRING(RACCT,4,1) = 0
				THEN 'Vadesiz'
			WHEN  SUBSTRING(RACCT,4,1) = 1  
				THEN 'Vadesiz'
			WHEN  SUBSTRING(RACCT,4,1) = 2  
				THEN 'Vadeli'
			WHEN  SUBSTRING(RACCT,4,1) = 3  
				THEN 'Vadeli'
			WHEN  SUBSTRING(RACCT,4,1) = 4  
				THEN 'Bloke Vadeli'
			WHEN  SUBSTRING(RACCT,4,1) = 5  
				THEN 'Bloke Vadesiz'
			WHEN  SUBSTRING(RACCT,4,1) = 6  
				THEN 'Teminat Vadeli'
			WHEN  SUBSTRING(RACCT,4,1) = 7 
				THEN 'Teminat Vadesiz'
			WHEN  SUBSTRING(RACCT,4,1) = 8 
				THEN 'KKM'
			ELSE 'Diğer'
			END  END AS HESAP_TIPI_TANIMI
	,static_comp.KREDIGRUBU
	,static_comp.CONTRIBUTEGROUP
	,static_comp.YK_SEKTOR
	,static_comp.YK_ULKE
	,static_comp.YK_KREDIGRUBU
	,static_comp.KREDIKATEGORISI
	,static_comp.YK_KREDIKISITI
	,static_comp.KA_KREDIGRUBU
	,static_comp.KA_KREDIKISITI
	,static_comp.CASH_GRUP1
	,static_comp.CASH_GRUP2
	,static_comp.CASH_GRUP3
	,static_comp.CASH_GRUP4
FROM {{ ref('stg__fi_kpi_t_fact_s4basemevduat') }} sfb --Şirketler Başlangıcı
	LEFT JOIN {{ ref('stg__fi_kpi_v_dim_s4mevduatcompanies') }} s4_comp ON sfb.RBUKRS = s4_comp.BUKRS
	LEFT JOIN {{ source('stg_fi_kpi', 'raw__fi_kpi_t_dim_staticmevduatcompanies') }} static_comp ON static_comp.BUKRS = sfb.RBUKRS
WHERE 1=1 AND
	(static_comp.HARICTUT IS NULL OR static_comp.HARICTUT = '')
	AND (s4_comp.LAND1 NOT IN ('RU','QA','NL'))
	OR (
		static_comp.HARICTUT IS NULL
		OR static_comp.HARICTUT = ''
		)
	AND (s4_comp.BUKRS = 'MED')


