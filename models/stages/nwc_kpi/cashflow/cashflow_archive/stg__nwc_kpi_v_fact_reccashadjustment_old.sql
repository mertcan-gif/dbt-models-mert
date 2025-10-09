
{{
  config(
    materialized = 'view',tags = ['nwc_kpi_old','cashflow_old','cf_adj']
    )
}}

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

















