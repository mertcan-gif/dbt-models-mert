
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','arap_2']
    )
}}

WITH RAW_CTE AS
(

	SELECT
		company = ACDOCA.RBUKRS, --Şirket Kodu
		document_number = ACDOCA.BELNR,
		document_line_item = ACDOCA.BUZEI,
		fiscal_year = ACDOCA.GJAHR,
		document_type = ACDOCA.BLART,
		posting_date = ACDOCA.BUDAT,
		document_date = ACDOCA.BLDAT,
		entry_date = BKPF.CPUDT,
		main_account = LEFT(RACCT,3), -- Alacak Türü
		customer_code = CASE 
					WHEN KNA1.KUNNR LIKE 'HR%' THEN REPLACE(KNA1.KUNNR,'HR','')
					ELSE KNA1.KUNNR END, -- Müşteri Kodu
		customer = KNA1.NAME1, -- Müşteri Adı
		business_area = ACDOCA.RBUSA, --İş Alanı
		business_area_description = T001W.NAME1, -- Proje Adı
		document_currency = RWCUR, --Döviz Cinsi
		--SHKZG,
		amount_in_document_currency = CASE
				  WHEN TCURX.CURRDEC = 3 THEN WSL/10 
			  ELSE WSL END,
		amount_in_company_currency = HSL,
		due_date = CASE WHEN NETDT <> 00000000 THEN CAST(NETDT AS DATE) ELSE NULL END,
		due_days = DATEDIFF(DAY,CAST(GETDATE() AS DATE),
					CASE WHEN NETDT <> 00000000 THEN CAST(NETDT AS DATE) ELSE NULL END)
		/**
			Aktif Pasif projelerin filtrelenmesi için aşağıdaki kolon eklenmiştir.
			Aktif Pasif bilgisi RTI'nın bütçelerinden alındığı için, RTI'ya dahil olmayan
			tüm projeler için 'ACTIVE' değeri atanmıştır.
		**/
		,is_active = CASE
						WHEN RBUSA IN (SELECT DISTINCT gsber  FROM [aws_stage].[sharepoint].raw__nwc_kpi_t_dim_rtibudgets) THEN 'ACTIVE'
						WHEN kuc.KyribaGrup LIKE '%RTI%' AND RBUSA NOT IN (SELECT DISTINCT gsber  FROM [aws_stage].[sharepoint].raw__nwc_kpi_t_dim_rtibudgets) THEN 'PASSIVE'
						ELSE 'ACTIVE'
					END
		,[source] = 'SAP'

		--,ACDOCA.*
 	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} AS ACDOCA
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} AS KNA1 ON ACDOCA.KUNNR = KNA1.KUNNR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} AS T001W ON ACDOCA.RBUSA = T001W.WERKS
		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} AS BKPF ON
					ACDOCA.BELNR = BKPF.BELNR 
					AND ACDOCA.RBUKRS = BKPF.BUKRS
					AND ACDOCA.GJAHR = BKPF.GJAHR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} AS TCURX ON ACDOCA.RWCUR = TCURX.CURRKEY
		LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON ACDOCA.RBUKRS = kuc.RobiKisaKod
	WHERE 1=1
		AND LEFT(RACCT,3) IN ('120')
		--AND CAST(H_BUDAT AS date) <= '2023-05-31'
		AND LEN(ACDOCA.KUNNR)<>3
		AND BKPF.STBLG <> 'X'
		AND ACDOCA.KUNNR <> ''
		--AND ACDOCA.AUGBL = ''

UNION ALL


SELECT
	RBUKRS = CASE WHEN rbukrs = 'BLN' THEN 'NS_BLN' END , --Şirket Kodu
	BELNR = belnr ,
	BUZEI = buzei,
	GJAHR = gjahr,
	BLART = '',
	BUDAT = CAST(budat AS nvarchar),
	BLDAT = CAST(bldat AS nvarchar),
	CPUDT = '',
	MAINACCOUNT = main_account, -- Alacak Türü
	KUNNR = kunnr, -- Müşteri Kodu
	CUSTOMER = customer , -- Müşteri Adı
	RBUSA = CONCAT('BNAR_',CAST(DENSE_RANK() OVER(ORDER BY project_name) AS NVARCHAR)) , --İş Alanı
	PROJECTNAME = project_name , -- Proje Adı
	RTCUR = rtcur , --Döviz Cinsi
	WSL = wsl,
	HSL = '',
	DUEDATE = CAST(CONCAT(LEFT(due_date,4),'-',RIGHT(LEFT(due_date,6),2),'-',LEFT(due_date,2)) AS DATE),
	DUE_DAYS = DATEDIFF(DAY,CAST(GETDATE() AS DATE),
				CAST(CONCAT(LEFT(due_date,4),'-',RIGHT(LEFT(due_date,6),2),'-',LEFT(due_date,2)) AS DATE))
	/**
		Aktif Pasif projelerin filtrelenmesi için aşağıdaki kolon eklenmiştir.
		Aktif Pasif bilgisi RTI'nın bütçelerinden alındığı için, RTI'ya dahil olmayan
		tüm projeler için 'ACTIVE' değeri atanmıştır.
	**/
	,ISACTIVE = 'ACTIVE'
	,[SOURCE] = 'OUTSOURCE'
FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_bnaccountsreceivable') }}

UNION ALL


SELECT
	RBUKRS = CASE 
				WHEN RBUKRS = 'RAC' THEN 'NS_RAC' 
				WHEN RBUKRS = 'RAV' THEN 'ARN'
				WHEN RBUKRS = 'ETS' THEN 'NS_RTB' 
				ELSE RBUKRS 
			END  , --Şirket Kodu
	BELNR = BELNR ,
	BUZEI,
	GJAHR,
	BLART = '',
	BUDAT = Convert(nvarchar,BUDAT+1,112),
	BLDAT = Convert(nvarchar,BLDAT+1,112),
	CPUDT = '',
	MAINACCOUNT, -- Alacak Türü
	KUNNR, -- = TRIM(REPLACE(KUNNR,'-','')), -- Müşteri Kodu
	CUSTOMER = CUSTOMER , -- Müşteri Adı
	RBUSA = RBUSA , --İş Alanı
	PROJECTNAME, -- Proje Adı
	RTCUR = RTCUR , --Döviz Cinsi
	WSL,
	HSL = '',
	DUEDATE = CAST(DUEDATE AS DATE),
	DUE_DAYS = DATEDIFF(DAY,CAST(GETDATE() AS DATE),CAST(DUEDATE AS DATE))
	/**
		Aktif Pasif projelerin filtrelenmesi için aşağıdaki kolon eklenmiştir.
		Aktif Pasif bilgisi RTI'nın bütçelerinden alındığı için, RTI'ya dahil olmayan
		tüm projeler için 'ACTIVE' değeri atanmıştır.
	**/
	,ISACTIVE = 'ACTIVE'
	,[SOURCE] = 'OUTSOURCE'
FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retaccountsreceivable') }}


UNION ALL


SELECT
	RBUKRS = CASE WHEN RBUKRS = 'HTK' THEN 'NS_HKP02' END , --Şirket Kodu
	BELNR = BELNR ,
	BUZEI,
	GJAHR,
	BLART = '',
	BUDAT = CAST(FORMAT(CAST(BUDAT AS DATE), 'yyyyMMdd') AS nvarchar),
	BLDAT = CAST(FORMAT(CAST(BLDAT AS DATE), 'yyyyMMdd') AS nvarchar),
	CPUDT = '',
	MAINACCOUNT, -- Alacak Türü
	KUNNR = LIFNR, -- Müşteri Kodu
	CUSTOMER = VENDOR , -- Müşteri Adı
	RBUSA = RBUSA , --İş Alanı
	PROJECTNAME, -- Proje Adı
	RTCUR = RTCUR , --Döviz Cinsi
	WSL,
	HSL = '',
	DUEDATE = CAST(DUEDATE AS DATE),
	DUE_DAYS = DATEDIFF(DAY,CAST(GETDATE() AS DATE),CAST(DUEDATE AS DATE))
	/**
		Aktif Pasif projelerin filtrelenmesi için aşağıdaki kolon eklenmiştir.
		Aktif Pasif bilgisi RTI'nın bütçelerinden alındığı için, RTI'ya dahil olmayan
		tüm projeler için 'ACTIVE' değeri atanmıştır.
	**/
	,ISACTIVE = 'ACTIVE'
	,[SOURCE] = 'OUTSOURCE'
FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_hkpaccountsreceivable') }}



	)

SELECT
	[rls_region]   = kuc.RegionCode
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(RAW_CTE.company  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(RAW_CTE.business_area  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,RAW_CTE.*
	,CASE WHEN due_days<0 THEN 'Overdue' ELSE 'Outstanding' END AS receivable_category
	,CASE
		WHEN ABS(due_days) <=30 THEN '0-30 Days'
		WHEN ABS(due_days) <=60 THEN '31-60 Days'
		WHEN ABS(due_days) <=90 THEN '61-90 Days'
		WHEN ABS(due_days) <=180 THEN '91-180 Days'
		WHEN ABS(due_days) <=365 THEN '181-365 Days'
		ELSE '>365 Days'
	END as due_category
	,transaction_type = 'Account Receivable'
	,turnover_ratio = NULL
	,kyriba_group = kuc.KyribaGrup
	,kyriba_company_code = kuc.KyribaKisaKod
FROM RAW_CTE
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON RAW_CTE.company = kuc.RobiKisaKod 