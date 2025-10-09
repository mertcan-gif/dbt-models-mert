{{
  config(
    materialized = 'view',tags = ['nwc_kpi','cashflow']
    )
}}
	SELECT 
		 [fiscal_year] = gjahr
		,[general_ledger_account] = hkont
		,[business_area] = gsber
		,gtext
		,[posting_date] = h_budat
		,[document_number] = belnr
		,[company] = bukrs
		,[document_date] = h_bldat
	/**
		Nakit Devir ve Nakit Düzeltme REC excellerinden gelen veriler için giriş tarihinin Kayıt Tarihinden alınabileceği Burak Aydın Bey ile birlikte netleştirilmiştir.
	**/
		,[entry_date] = CAST(record_date AS DATE) 
		,[document_line_item] = buzei
		,[document_type] = h_blart 
		,offsetting_account_number = gkont 
		,item_text = sgtxt 
		,[general_ledger] =  main_account 
		,amount_in_bp = balance_bp
		,document_currency = h_waers
		,amount_transaction_currency = ''
		,amount_in_tl = balance_tl 
		,amount_in_usd = balance_dollar
		,amount_in_eur = balance_euro 
		,amount_notification = NULL
		,category = cash_type
		,financial_item
		,[group]
		,first_definition = description_1
		,[definition] = [description]
		,account_code
		,account_name
		,xreversing = ''
		,xreversed = ''
		,reverse_document_number = ''
		,username = ''
		,[type]
		,CAST('' AS NVARCHAR) AS ptext
		,CAST('' AS NVARCHAR) AS [high_level_breakdown] 
		,[interest_calculation_date] = CAST(h_bldat AS DATE)
		,[interest_period] = CONCAT(YEAR(CAST(h_bldat AS DATE)),RIGHT(LEFT(CAST(h_bldat AS DATE),7),2))
		,[included_in_o_n_interest] = 'X'
		,'YES' as is_adjusting_document
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_cashturnover2022') }}