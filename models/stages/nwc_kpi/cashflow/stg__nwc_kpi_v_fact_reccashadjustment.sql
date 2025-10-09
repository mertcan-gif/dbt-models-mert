
{{
  config(
    materialized = 'view',tags = ['nwc_kpi','cashflow','cf_adj']
    )
}}

	SELECT 
		 fiscal_year = gjahr
		,general_ledger_account = hkont
		,business_area = gsber
		,gtext
		,posting_date  = h_budat
		,document_number = belnr
		,company = bukrs
		,document_date = h_bldat 
	/**
		Nakit Devir ve Nakit Düzeltme REC excellerinden gelen veriler için giriş tarihinin Kayıt Tarihinden alınabileceği Burak Aydın Bey ile birlikte netleştirilmiştir.
	**/
		,entry_date = CAST(record_date  AS DATE)
		,document_line_item = buzei
		,document_type = h_blart
		,offsetting_account_number = CAST(gkont AS NVARCHAR)
		,item_text = sgtxt
		,general_ledger = LEFT(hkont,3)
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
		,account_name = [name]
		,xreversing = ''
		,xreversed = ''
		,reverse_document_number = ''
		,username = ''
		,[type]
		,ptext
		,high_level_breakdown = upper_break
		,interest_calculation_date = CAST([Faiz Hesaplanma Tarihi] AS DATE)
		,interest_period = [Faiz Dönemi]
		,included_in_o_n_interest = [O/N Faiz Hesabına Dahil]
		,'YES' as is_adjusting_document
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_reccashadjustmentgeneral') }}

	UNION ALL 

	SELECT 
		 [GJAHR] = gjahr
		,[HKONT] = hkont
		,[GSBER] = gsber
		,[GTEXT] = gtext
		,[BUDAT]  = h_budat
		,[BELNR] = belnr
		,[BUKRS] = bukrs
		,[BLDAT] = h_bldat 
	/**
		Nakit Devir ve Nakit Düzeltme REC excellerinden gelen veriler için giriş tarihinin Kayıt Tarihinden alınabileceği Burak Aydın Bey ile birlikte netleştirilmiştir.
	**/
		,[Giriş Tar] = CAST(record_date  AS DATE)
		,[BUZEI] = buzei
		,[BLART] = h_blart
		,[GKONT] = CAST(gkont AS NVARCHAR)
		,[SGTXT] = sgtxt
		,[Kebir] = LEFT(hkont,3)
		,balance_bp
		,H_WAERS = h_waers
		,rtcur = ''
		,balance_tl 
		,balance_dollar
		,balance_euro 
		,teblig_tutar = NULL
		,[Tür] = cash_type
		,[MALİ KALEM] =financial_item
		,[GRUP] = [group]
		,[TANIM 1] = description_1
		,[TANIMI] = [description]
		,[Cari] = account_code
		,[Adı] = [name]
		,xreversing = ''
		,xreversed = ''
		,stblg = ''
		,usnam = ''
		,[Tipi] = [type]
		,[PTEXT] = ptext
		,[ÜST KIRILIM] = upper_break
		,[Faiz Hesaplanma Tarihi] = CAST([Faiz Hesaplanma Tarihi] AS DATE)
		,[Faiz Dönemi]
		,[O/N Faiz Hesabına Dahil]
		,'YES' as is_adjusting_document
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_reccashadjustmentinterest') }}

	UNION ALL 

	SELECT 
		 [GJAHR] = gjahr
		,[HKONT] = hkont
		,[GSBER] = gsber
		,[GTEXT] = gtext
		,[BUDAT]  = h_budat
		,[BELNR] = belnr
		,[BUKRS] = bukrs
		,[BLDAT] = h_bldat 
	/**
		Nakit Devir ve Nakit Düzeltme REC excellerinden gelen veriler için giriş tarihinin Kayıt Tarihinden alınabileceği Burak Aydın Bey ile birlikte netleştirilmiştir.
	**/
		,[Giriş Tar] = CAST(record_date  AS DATE)
		,[BUZEI] = buzei
		,[BLART] = h_blart
		,[GKONT] = CAST(gkont AS NVARCHAR)
		,[SGTXT] = sgtxt
		,[Kebir] = LEFT(hkont,3)
		,balance_bp
		,H_WAERS = h_waers
		,rtcur = ''
		,balance_tl 
		,balance_dollar
		,balance_euro 
		,teblig_tutar = NULL
		,[Tür] = cash_type
		,[MALİ KALEM] =financial_item
		,[GRUP] = [group]
		,[TANIM 1] = description_1
		,[TANIMI] = [description]
		,[Cari] = account_code
		,[Adı] = [name]
		,xreversing = ''
		,xreversed = ''
		,stblg = ''
		,usnam = ''
		,[Tipi] = [type]
		,[PTEXT] = ptext
		,[ÜST KIRILIM] = upper_break
		,[Faiz Hesaplanma Tarihi] = CAST([Faiz Hesaplanma Tarihi] AS DATE)
		,[Faiz Dönemi]
		,[O/N Faiz Hesabına Dahil]
		,'YES' as is_adjusting_document
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_reccashadjustmenthedge') }}

	UNION ALL 

	SELECT 
		 [GJAHR] = gjahr
		,[HKONT] = hkont
		,[GSBER] = gsber
		,[GTEXT] = gtext
		,[BUDAT]  = h_budat
		,[BELNR] = belnr
		,[BUKRS] = bukrs
		,[BLDAT] = h_bldat 
	/**
		Nakit Devir ve Nakit Düzeltme REC excellerinden gelen veriler için giriş tarihinin Kayıt Tarihinden alınabileceği Burak Aydın Bey ile birlikte netleştirilmiştir.
	**/
		,[Giriş Tar] = CAST(record_date  AS DATE)
		,[BUZEI] = buzei
		,[BLART] = h_blart
		,[GKONT] = CAST(gkont AS NVARCHAR)
		,[SGTXT] = sgtxt
		,[Kebir] = LEFT(hkont,3)
		,balance_bp
		,H_WAERS = h_waers
		,rtcur = ''
		,balance_tl 
		,balance_dollar
		,balance_euro 
		,teblig_tutar = NULL
		,[Tür] = cash_type
		,[MALİ KALEM] =financial_item
		,[GRUP] = [group]
		,[TANIM 1] = description_1
		,[TANIMI] = [description]
		,[Cari] = account_code
		,[Adı] = [name]
		,xreversing = ''
		,xreversed = ''
		,stblg = ''
		,usnam = ''
		,[Tipi] = [type]
		,[PTEXT] = ptext
		,[ÜST KIRILIM] = upper_break
		,[Faiz Hesaplanma Tarihi] = CAST([Faiz Hesaplanma Tarihi] AS DATE)
		,[Faiz Dönemi]
		,[O/N Faiz Hesabına Dahil]
		,'YES' as is_adjusting_document
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_reccashadjustmentgyg') }}

	UNION ALL 

	SELECT 
		 [GJAHR] = gjahr
		,[HKONT] = hkont
		,[GSBER] = gsber
		,[GTEXT] = gtext
		,[BUDAT]  = h_budat
		,[BELNR] = belnr
		,[BUKRS] = bukrs
		,[BLDAT] = h_bldat 
	/**
		Nakit Devir ve Nakit Düzeltme REC excellerinden gelen veriler için giriş tarihinin Kayıt Tarihinden alınabileceği Burak Aydın Bey ile birlikte netleştirilmiştir.
	**/
		,[Giriş Tar] = CAST(record_date  AS DATE)
		,[BUZEI] = buzei
		,[BLART] = h_blart
		,[GKONT] = CAST(gkont AS NVARCHAR)
		,[SGTXT] = sgtxt
		,[Kebir] = LEFT(hkont,3)
		,balance_bp
		,H_WAERS = h_waers
		,rtcur = ''
		,balance_tl 
		,balance_dollar
		,balance_euro 
		,teblig_tutar = NULL
		,[Tür] = cash_type
		,[MALİ KALEM] =financial_item
		,[GRUP] = [group]
		,[TANIM 1] = description_1
		,[TANIMI] = [description]
		,[Cari] = account_code
		,[Adı] = [name]
		,xreversing = ''
		,xreversed = ''
		,stblg = ''
		,usnam = ''
		,[Tipi] = [type]
		,[PTEXT] = ptext
		,[ÜST KIRILIM] = upper_break
		,[Faiz Hesaplanma Tarihi] = CAST([Faiz Hesaplanma Tarihi] AS DATE)
		,[Faiz Dönemi]
		,[O/N Faiz Hesabına Dahil]
		,'YES' as is_adjusting_document
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_reccashadjustmentdevir') }}

















