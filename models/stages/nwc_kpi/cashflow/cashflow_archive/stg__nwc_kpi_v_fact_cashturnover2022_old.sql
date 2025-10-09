{{
  config(
    materialized = 'view',tags = ['nwc_kpi_old','cashflow_old']
    )
}}
	SELECT 
		 [GJAHR] = gjahr
		,[HKONT] = hkont
		,[GSBER] = gsber
		,[GTEXT] = gtext
		,[BUDAT] = h_budat
		,[BELNR] = belnr
		,[BUKRS] = bukrs
		,[BLDAT] = h_bldat
	/**
		Nakit Devir ve Nakit Düzeltme REC excellerinden gelen veriler için giriş tarihinin Kayıt Tarihinden alınabileceği Burak Aydın Bey ile birlikte netleştirilmiştir.
	**/
		,[Giriş Tar] = CAST(record_date AS DATE) 
		,[BUZEI] = buzei
		,[BLART] = h_blart 
		,GKONT = gkont 
		,SGTXT = sgtxt 
		,[Kebir] =  main_account 
		,balance_bp
		,H_WAERS = h_waers
		,rtcur = ''
		,balance_tl 
		,balance_dollar
		,balance_euro 
		,teblig_tutar = NULL
		,[Tür] = cash_type
		,[MALİ KALEM] = financial_item
		,[GRUP] = [group]
		,[TANIM 1] = description_1
		,[TANIMI] = [description]
		,[Cari] = account_code
		,[Adı] = account_name
		,xreversing = ''
		,xreversed = ''
		,stblg = ''
		,usnam = ''
		,[Tipi] = [type]
		,CAST('' AS NVARCHAR) AS ptext
		,CAST('' AS NVARCHAR) AS [ÜST KIRILIM] 
		,[Faiz Hesaplanma Tarihi] = CAST(h_bldat AS DATE)
		,[Faiz Dönemi] = CONCAT(YEAR(CAST(h_bldat AS DATE)),RIGHT(LEFT(CAST(h_bldat AS DATE),7),2))
		,[O/N Faiz Hesabına Dahil] = 'X'
		,'YES' as is_adjusting_document
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_cashturnover2022') }}