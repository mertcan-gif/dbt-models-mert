
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','cashflow']
    )
}}

WITH cf_updated AS (
SELECT
	    [rls_region]
      , [rls_group]
      , [rls_company]
      , [rls_businessarea]
      , fiscal_year
      , general_ledger_account
      , business_area
      , business_area_description
      , posting_date
      , document_number
      , company
      , document_date
      , entry_date
	  , xreversing 
	  , xreversed 
	  , reverse_document_number
	  , username 
      , document_line_item
      , document_type
      , offsetting_account_number
      , item_text
      , general_ledger
      , amount_in_document_currency
      , document_currency
      , amount_transaction_currency
      , amount_in_tl
      , amount_in_usd
      , amount_in_eur
      , amount_notification
      , [category]
      , commitment_item
      , [group]
      , [first_definition]
      , [definition]
      , account_code
      , account_name
      , [type]
      , ptext
      , high_level_breakdown
	  , interest_calculation_date
	  , interest_period
	  , included_in_o_n_interest
	  , is_adjusting_document
	  , mapping_index = 
		CASE
			WHEN business_area IN ('R053','R022','R039','R027') AND document_type = 'S1' and company = 'REC'																		THEN 1
			WHEN business_area IN ('R053','R022','R039','R027') AND document_type = 'S2' and company = 'REC'																		THEN 2
			WHEN company = 'REC' AND business_area = 'M007'																												THEN 3
			WHEN document_type = 's1' and account_code = '0005000320'																									THEN 4
			WHEN document_type = 's2' and account_code = '0005000320'																									THEN 5
			WHEN LOWER([type]) = 'kar'																															THEN 6
			WHEN LOWER(item_text) like '%leasing%' and (RIGHT(UPPER(business_area),1) != 'M' AND RIGHT(UPPER(business_area),1) != 'T')												THEN 7
			WHEN (LOWER([type]) = 'gider' OR LOWER([type]) = 'gıder') and (RIGHT(UPPER(business_area),1) != 'M' AND RIGHT(UPPER(business_area),1) != 'T')							THEN 8
			WHEN (LOWER([type]) = 'gelir' OR LOWER([type]) = 'gelır') and (RIGHT(UPPER(business_area),1) != 'M' AND RIGHT(UPPER(business_area),1) != 'T')							THEN 9
			WHEN item_text like '%KKM%'																																THEN 10
			WHEN item_text like '%NDF%' AND commitment_item = '649100'																								THEN 11
			WHEN lower(item_text) like '%hazine%' or lower(item_text) like '%hznm%' or lower(item_text) like '%netoff%' AND document_type = 'S1'									THEN 12
			WHEN lower(item_text) like '%hazine%' or lower(item_text) like '%hznm%' or lower(item_text) like '%netoff%' AND document_type = 'S2'									THEN 13
			WHEN document_type = 'S2' AND commitment_item = '780001' AND LEN(account_code)=3																			THEN 14
			WHEN document_type = 'S2' AND commitment_item IN ('381010','281010') AND LEN(account_code)=3																THEN 15
			WHEN document_type = 'S2' AND commitment_item IN ('300101','400100','136060','336060') AND LEN(account_code)=3												THEN 16
			WHEN document_type = 'S1' AND commitment_item = '780001' AND LEN(account_code)=3																			THEN 17
			WHEN document_type = 'S1' AND commitment_item IN ('381010','281010') AND LEN(account_code)=3																THEN 18
			WHEN document_type = 'S1' AND commitment_item IN ('300101','400100','136060','336060') AND LEN(account_code)=3												THEN 19
			WHEN document_type = 'S1' AND LEN(account_code)=3																											THEN 20
			WHEN document_type = 'S2' AND LEN(account_code)=3																											THEN 21
			WHEN (LOWER(item_text) like '%cari bakiye%') and amount_in_document_currency<0																			THEN 22 
			WHEN (LOWER(item_text) like '%cari bakiye%') and amount_in_document_currency>0																			THEN 23 
			WHEN company = 'REC' AND item_text like '%FX%' AND offsetting_account_number IN ('1023538005','1022538007','1023538017','1021538039') and  amount_in_document_currency>0		THEN 24
			WHEN company = 'REC' AND item_text like '%FX%' AND offsetting_account_number IN ('1023538005','1022538007','1023538017','1021538039') and amount_in_document_currency<0		THEN 25
			WHEN company = 'REC' AND offsetting_account_number IN ('1023538005','1022538007','1023538017','1021538039') AND commitment_item = '642100' and document_type = 'S2'				THEN 26
			WHEN company = 'REC' AND offsetting_account_number IN ('1023538005','1022538007','1023538017','1021538039') AND commitment_item = '649100' and document_type = 'S2'				THEN 27
			WHEN company = 'REC' AND offsetting_account_number IN ('1023538005','1022538007','1023538017','1021538039') AND commitment_item = '300101' and document_type = 'S2'				THEN 28
			WHEN company = 'REC' AND offsetting_account_number IN ('1023538005','1022538007','1023538017','1021538039') AND commitment_item IN ('381010','331040') and document_type = 'S1'  THEN 29
			WHEN company = 'REC' AND offsetting_account_number IN ('1023538005','1022538007','1023538017','1021538039') AND commitment_item = '300101' and document_type = 'S1'				THEN 30
			WHEN company = 'RMI' AND item_text = 'FX' AND document_type = 'S2' AND offsetting_account_number IN ('1020538010'
																				,'1021538010'
																				,'1021538025'
																				,'1022538010'
																				,'1023538007'
																				,'1023538021' )	
																	and  amount_in_document_currency>0															THEN 31
			WHEN company = 'RMI' AND item_text = 'FX' AND document_type = 'S1' AND offsetting_account_number IN ('1020538010'
																				,'1021538010'
																				,'1021538025'
																				,'1022538010'
																				,'1023538007'
																				,'1023538021' )
																	and  amount_in_document_currency<0															THEN 32
			WHEN company = 'RMI' AND item_text = 'FX' AND document_type = 'S2' AND offsetting_account_number IN ('1020538010'
																				,'1021538010'
																				,'1021538025'
																				,'1022538010'
																				,'1023538007'
																				,'1023538021' )
																  AND commitment_item = '642100'																THEN 33
			WHEN company = 'RMI' AND item_text = 'FX' AND document_type = 'S2' AND offsetting_account_number IN ('1020538010'
																				,'1021538010'
																				,'1021538025'
																				,'1022538010'
																				,'1023538007'
																				,'1023538021' )
																  AND commitment_item = '649100'																THEN 34
			WHEN  company = 'RMI' AND document_type = 'S2' AND offsetting_account_number IN ('1020538010'
																			,'1021538010'
																			,'1021538025'
																			,'1022538010'
																			,'1023538007'
																			,'1023538021' )
																AND commitment_item = '300101'																	THEN 35
			WHEN company = 'RMI' AND document_type = 'S1' AND offsetting_account_number IN ('1020538010'
																			,'1021538010'
																			,'1021538025'
																			,'1022538010'
																			,'1023538007'
																			,'1023538021' )
																AND commitment_item IN ('381010','331040')														THEN 36
			WHEN company = 'RMI' AND document_type = 'S1' AND offsetting_account_number IN ('1020538010'
																			,'1021538010'
																			,'1021538025'
																			,'1022538010'
																			,'1023538007'
																			,'1023538021' )
																AND commitment_item = '300101'																	THEN 37
			WHEN item_text like '%FX%' AND document_type = 'S3' and amount_in_document_currency<0																			THEN 38
			WHEN item_text like '%FX%' AND document_type = 'S3'	and amount_in_document_currency>0																			THEN 39
			WHEN upper(item_text) like N'%DÖVİZ SATIŞ%' or lower(item_text) like '%döviz satis%' or lower(item_text) like '%döviz satış%'	or lower(item_text) like '%dovız satıs%' and amount_in_document_currency<0	THEN 40
			WHEN upper(item_text) like N'%DÖVİZ ALIŞ%' or lower(item_text) like '%döviz alis%'	OR lower(item_text) like '%döviz alış%' or lower(item_text) like '%dovız alıs%'	and amount_in_document_currency>0		THEN 41
			WHEN document_type = 'S1' AND commitment_item IN ('300101','400100')																						THEN 42
			WHEN document_type = 'S1' AND commitment_item IN ('780001','770030')																						THEN 43
			WHEN document_type = 'S1' AND commitment_item = '381010'																									THEN 44
			WHEN document_type = 'S2' AND commitment_item IN ('300101','400100')																						THEN 45
			WHEN commitment_item = '642100' 																													THEN 46
			WHEN (LEFT(general_ledger_account,3) = '360')																														THEN 47
			WHEN document_type = 'UE' 																																	THEN 48
			WHEN (LEFT(general_ledger_account,3) = '102' AND general_ledger_account != '1029999999' and LEFT(offsetting_account_number,3) = '102') OR (LOWER(item_text) like '%virman%'  OR
																								 LOWER(item_text) like '%vadesiz%' OR
																								 LOWER(item_text) like '%vadeli%' )									THEN 49
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
  FROM {{ ref('stg__nwc_kpi_t_fact_cashflowadjusted') }}
)

SELECT   cf_updated.*
		,mapping.budget
		,mapping.budget_layout
		,kyriba_mapping = mapping.[kyriba_mappping]
		,mapping.[kyriba_mapping_description]
		,mapping.[cf_name]
		,mapping.[relevant_team]
		,business_area_concatted = CONCAT(business_area,' - ',business_area_description)
FROM cf_updated
	LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_cashflowkyribaautomationmapping') }} mapping ON cf_updated.mapping_index = mapping.mapping_index



