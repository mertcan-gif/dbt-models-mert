
{{
  config(
    materialized = 'table',tags = ['nwc_kpi_draft']
    )
}}

WITH RAW_CTE AS
(

	SELECT
		ACDOCA.RBUKRS, --Şirket Kodu
		ACDOCA.BELNR,
		ACDOCA.BUZEI,
		ACDOCA.GJAHR,
		ACDOCA.BLART,
		ACDOCA.BUDAT,
		ACDOCA.BLDAT,
		BKPF.CPUDT,
		MAINACCOUNT = LEFT(RACCT,3), -- Alacak Türü
		KUNNR = CASE 
					WHEN KNA1.KUNNR LIKE 'HR%' THEN REPLACE(KNA1.KUNNR,'HR','')
					ELSE KNA1.KUNNR END, -- Müşteri Kodu
		KNA1.NAME1 AS CUSTOMER, -- Müşteri Adı
		ACDOCA.RBUSA, --İş Alanı
		T001W.NAME1 AS PROJECTNAME, -- Proje Adı
		RWCUR, --Döviz Cinsi
		--SHKZG,
		WSL = CASE
				  WHEN TCURX.CURRDEC = 3 THEN WSL/10 
			  ELSE WSL END,
		HSL,
		DUEDATE = CASE WHEN NETDT <> 00000000 THEN CAST(NETDT AS DATE) ELSE NULL END,
		DUE_DAYS = DATEDIFF(DAY,CAST(GETDATE() AS DATE),
					CASE WHEN NETDT <> 00000000 THEN CAST(NETDT AS DATE) ELSE NULL END)
		/**
			Aktif Pasif projelerin filtrelenmesi için aşağıdaki kolon eklenmiştir.
			Aktif Pasif bilgisi RTI'nın bütçelerinden alındığı için, RTI'ya dahil olmayan
			tüm projeler için 'ACTIVE' değeri atanmıştır.
		**/
		,ISACTIVE = CASE
						WHEN RBUSA IN (SELECT DISTINCT gsber  FROM [aws_stage].[sharepoint].raw__nwc_kpi_t_dim_rtibudgets) THEN 'ACTIVE'
						WHEN kuc.KyribaGrup LIKE '%RTI%' AND RBUSA NOT IN (SELECT DISTINCT gsber  FROM [aws_stage].[sharepoint].raw__nwc_kpi_t_dim_rtibudgets) THEN 'PASSIVE'
						ELSE 'ACTIVE'
					END
		,[SOURCE] = 'SAP'

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
	,[rls_company] = CONCAT(COALESCE(RAW_CTE.RBUKRS  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(RAW_CTE.RBUSA  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,RAW_CTE.*
	,CASE WHEN DUE_DAYS<0 THEN 'Overdue' ELSE 'Outstanding' END AS RECEIVABLE_CATEGORY
	,CASE
		WHEN ABS(DUE_DAYS) <=30 THEN '0-30 Days'
		WHEN ABS(DUE_DAYS) <=60 THEN '31-60 Days'
		WHEN ABS(DUE_DAYS) <=90 THEN '61-90 Days'
		WHEN ABS(DUE_DAYS) <=180 THEN '91-180 Days'
		WHEN ABS(DUE_DAYS) <=365 THEN '181-365 Days'
		ELSE '>365 Days'
	END as DUE_CATEGORY
	,TRANSACTION_TYPE = 'Account Receivable'
	,TURNOVER_RATIO = NULL
	,kuc.KyribaGrup
	,kuc.KyribaKisaKod
FROM RAW_CTE
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON RAW_CTE.RBUKRS = kuc.RobiKisaKod 