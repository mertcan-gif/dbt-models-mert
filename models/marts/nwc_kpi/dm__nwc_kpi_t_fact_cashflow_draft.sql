
{{
  config(
    materialized = 'table',tags = ['nwc_kpi_draft','cashflow_old']
    )
}}

WITH cf_updated AS (
SELECT
	    [rls_region]
      , [rls_group]
      , [rls_company]
      , [rls_businessarea]
      , [gjahr] AS GJAHR
      , [racct] AS RACCT
      , [rbusa] AS RBUSA
      , [gtext] AS GTEXT
      , [budat] AS BUDAT
      , [belnr] AS BELNR
      , [rbukrs] AS RBUKRS
      , [bldat] AS BLDAT
      , [cpudt] AS CPUDT
	  ,xreversing 
	  ,xreversed 
	  ,stblg
	  ,usnam 
      , [buzei] AS BUZEI
      , [blart] AS BLART
      , [gkont] AS GKONT
      , [sgtxt] AS SGTXT
      , [ledger_account] AS Kebir
      , [amount_in_document_currency] AS [BAKİYE BP]
      , [rwcur] AS RWCUR
      , [rtcur] AS RTCUR
      , [amount_try] AS [BAKİYE TL]
      , [amount_usd] AS [BAKİYE DOLAR]
      , [amount_eur] AS [BAKİYE EURO]
      , [amount_teblig] AS [BAKİYE TEBLIG]
      , [cf_type] AS Tür
      , [commitment_item] AS [MALİ KALEM]
      , [group] AS [GRUP]
      , [description_1] AS [TANIM 1]
      , [description] AS [TANIMI]
      , [account_code] AS [Cari]
      , [account_name] AS [Adı]
      , [Tipi]
      , [ptext]
      , [ÜST KIRILIM]
      , [BELGE TARİHİ]
	  , [Faiz Hesaplanma Tarihi]
	  , [Faiz Dönemi]
	  , [O/N Faiz Hesabına Dahil]
	  , is_adjusting_document
		,mapping_index = 
		CASE
			WHEN rbusa IN ('R053','R022','R039','R027') AND blart = 'S1' and rbukrs = 'REC'																		THEN 1
			WHEN rbusa IN ('R053','R022','R039','R027') AND blart = 'S2' and rbukrs = 'REC'																		THEN 2
			WHEN rbukrs = 'REC' AND rbusa = 'M007'																												THEN 3
			WHEN blart = 's1' and account_code = '0005000320'																									THEN 4
			WHEN blart = 's2' and account_code = '0005000320'																									THEN 5
			WHEN LOWER(Tipi) = 'kar'																															THEN 6
			WHEN LOWER(sgtxt) like '%leasing%' and (RIGHT(UPPER(rbusa),1) != 'M' AND RIGHT(UPPER(rbusa),1) != 'T')												THEN 7
			WHEN (LOWER(Tipi) = 'gider' OR LOWER(Tipi) = 'gıder') and (RIGHT(UPPER(rbusa),1) != 'M' AND RIGHT(UPPER(rbusa),1) != 'T')							THEN 8
			WHEN (LOWER(Tipi) = 'gelir' OR LOWER(Tipi) = 'gelır') and (RIGHT(UPPER(rbusa),1) != 'M' AND RIGHT(UPPER(rbusa),1) != 'T')							THEN 9
			WHEN sgtxt like '%KKM%'																																THEN 10
			WHEN sgtxt like '%NDF%' AND commitment_item = '649100'																								THEN 11
			WHEN lower(sgtxt) like '%hazine%' or lower(sgtxt) like '%hznm%' or lower(sgtxt) like '%netoff%' AND blart = 'S1'									THEN 12
			WHEN lower(sgtxt) like '%hazine%' or lower(sgtxt) like '%hznm%' or lower(sgtxt) like '%netoff%' AND blart = 'S2'									THEN 13
			WHEN blart = 'S2' AND commitment_item = '780001' AND LEN(account_code)=3																			THEN 14
			WHEN blart = 'S2' AND commitment_item IN ('381010','281010') AND LEN(account_code)=3																THEN 15
			WHEN blart = 'S2' AND commitment_item IN ('300101','400100','136060','336060') AND LEN(account_code)=3												THEN 16
			WHEN blart = 'S1' AND commitment_item = '780001' AND LEN(account_code)=3																			THEN 17
			WHEN blart = 'S1' AND commitment_item IN ('381010','281010') AND LEN(account_code)=3																THEN 18
			WHEN blart = 'S1' AND commitment_item IN ('300101','400100','136060','336060') AND LEN(account_code)=3												THEN 19
			WHEN blart = 'S1' AND LEN(account_code)=3																											THEN 20
			WHEN blart = 'S2' AND LEN(account_code)=3																											THEN 21
			WHEN (LOWER(sgtxt) like '%cari bakiye%') and amount_in_document_currency<0																			THEN 22 
			WHEN (LOWER(sgtxt) like '%cari bakiye%') and amount_in_document_currency>0																			THEN 23 
			WHEN rbukrs = 'REC' AND sgtxt like '%FX%' AND gkont IN ('1023538005','1022538007','1023538017','1021538039') and  amount_in_document_currency>0		THEN 24
			WHEN rbukrs = 'REC' AND sgtxt like '%FX%' AND gkont IN ('1023538005','1022538007','1023538017','1021538039') and amount_in_document_currency<0		THEN 25
			WHEN rbukrs = 'REC' AND gkont IN ('1023538005','1022538007','1023538017','1021538039') AND commitment_item = '642100' and blart = 'S2'				THEN 26
			WHEN rbukrs = 'REC' AND gkont IN ('1023538005','1022538007','1023538017','1021538039') AND commitment_item = '649100' and blart = 'S2'				THEN 27
			WHEN rbukrs = 'REC' AND gkont IN ('1023538005','1022538007','1023538017','1021538039') AND commitment_item = '300101' and blart = 'S2'				THEN 28
			WHEN rbukrs = 'REC' AND gkont IN ('1023538005','1022538007','1023538017','1021538039') AND commitment_item IN ('381010','331040') and blart = 'S1'  THEN 29
			WHEN rbukrs = 'REC' AND gkont IN ('1023538005','1022538007','1023538017','1021538039') AND commitment_item = '300101' and blart = 'S1'				THEN 30
			WHEN rbukrs = 'RMI' AND sgtxt = 'FX' AND blart = 'S2' AND gkont IN ('1020538010'
																				,'1021538010'
																				,'1021538025'
																				,'1022538010'
																				,'1023538007'
																				,'1023538021' )	
																	and  amount_in_document_currency>0															THEN 31
			WHEN rbukrs = 'RMI' AND sgtxt = 'FX' AND blart = 'S1' AND gkont IN ('1020538010'
																				,'1021538010'
																				,'1021538025'
																				,'1022538010'
																				,'1023538007'
																				,'1023538021' )
																	and  amount_in_document_currency<0															THEN 32
			WHEN rbukrs = 'RMI' AND sgtxt = 'FX' AND blart = 'S2' AND gkont IN ('1020538010'
																				,'1021538010'
																				,'1021538025'
																				,'1022538010'
																				,'1023538007'
																				,'1023538021' )
																  AND commitment_item = '642100'																THEN 33
			WHEN rbukrs = 'RMI' AND sgtxt = 'FX' AND blart = 'S2' AND gkont IN ('1020538010'
																				,'1021538010'
																				,'1021538025'
																				,'1022538010'
																				,'1023538007'
																				,'1023538021' )
																  AND commitment_item = '649100'																THEN 34
			WHEN  rbukrs = 'RMI' AND blart = 'S2' AND gkont IN ('1020538010'
																			,'1021538010'
																			,'1021538025'
																			,'1022538010'
																			,'1023538007'
																			,'1023538021' )
																AND commitment_item = '300101'																	THEN 35
			WHEN rbukrs = 'RMI' AND blart = 'S1' AND gkont IN ('1020538010'
																			,'1021538010'
																			,'1021538025'
																			,'1022538010'
																			,'1023538007'
																			,'1023538021' )
																AND commitment_item IN ('381010','331040')														THEN 36
			WHEN rbukrs = 'RMI' AND blart = 'S1' AND gkont IN ('1020538010'
																			,'1021538010'
																			,'1021538025'
																			,'1022538010'
																			,'1023538007'
																			,'1023538021' )
																AND commitment_item = '300101'																	THEN 37
			WHEN sgtxt like '%FX%' AND blart = 'S3' and amount_in_document_currency<0																			THEN 38
			WHEN sgtxt like '%FX%' AND blart = 'S3'	and amount_in_document_currency>0																			THEN 39
			WHEN upper(sgtxt) like N'%DÖVİZ SATIŞ%' or lower(sgtxt) like '%döviz satis%' or lower(sgtxt) like '%döviz satış%'	or lower(sgtxt) like '%dovız satıs%' and amount_in_document_currency<0	THEN 40
			WHEN upper(sgtxt) like N'%DÖVİZ ALIŞ%' or lower(sgtxt) like '%döviz alis%'	OR lower(sgtxt) like '%döviz alış%' or lower(sgtxt) like '%dovız alıs%'	and amount_in_document_currency>0		THEN 41
			WHEN blart = 'S1' AND commitment_item IN ('300101','400100')																						THEN 42
			WHEN blart = 'S1' AND commitment_item IN ('780001','770030')																						THEN 43
			WHEN blart = 'S1' AND commitment_item = '381010'																									THEN 44
			WHEN blart = 'S2' AND commitment_item IN ('300101','400100')																						THEN 45
			WHEN commitment_item = '642100' 																													THEN 46
			WHEN (LEFT(racct,3) = '360')																														THEN 47
			WHEN blart = 'UE' 																																	THEN 48
			WHEN (LEFT(racct,3) = '102' AND racct != '1029999999' and LEFT(gkont,3) = '102') OR (LOWER(sgtxt) like '%virman%'  OR
																								 LOWER(sgtxt) like '%vadesiz%' OR
																								 LOWER(sgtxt) like '%vadeli%' )									THEN 49
			WHEN commitment_item IN ('368000'
									,'646101'
									,'656100'
									,'740010'
									,'195010'
									,'309000'
									,'361000'
									,'335000'
									,'193010'
									,'159122'
									,'770010'
									,'320100'
									,'679100'
									,'136020'
									,'360300'
									,'159221')																													THEN 50

		END
  FROM {{ ref('stg__nwc_kpi_t_fact_cashflowadjusted_old') }}
)

SELECT   cf_updated.*
		,mapping.budget
		,mapping.budget_layout
		,mapping.[kyriba_mappping]
		,mapping.[kyriba_mapping_description]
		,mapping.[cf_name]
		,mapping.[relevant_team]
FROM cf_updated
	LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_cashflowkyribaautomationmapping') }} mapping ON cf_updated.mapping_index = mapping.mapping_index



