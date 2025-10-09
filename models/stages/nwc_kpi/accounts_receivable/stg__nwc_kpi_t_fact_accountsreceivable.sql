
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','arap']
    )
}}

WITH dim_bn_customer AS (
	SELECT 
		Customer_NK
		,CustomerId
		,CustomerGroup
		,rn = ROW_NUMBER() OVER(PARTITION BY CustomerGroup ORDER BY CustomerId)
	FROM [PRDSYNDW-ONDEMAND.SQL.AZURESYNAPSE.NET].[ronesansdwh].[dbo].[dim_Customer] c
	WHERE 1=1
		AND CustomerGroup <> 'BN'
		AND CustomerGroup <> 'FE'
		
)

,RAW_CTE AS
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
		,company_code = ''
		,company_name = ''
		,in_out_group_type = CASE WHEN LEN(KNA1.KUNNR) = 3 THEN 'In Group' WHEN LEN(KNA1.KUNNR) <> 3 THEN 'Out Group' END
		--,[SOURCE] = 'SAP'

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
		AND ACDOCA.AUGBL = ''

--UNION ALL


--SELECT
--	RBUKRS = CASE WHEN rbukrs = 'BLN' THEN 'NS_BLN' END , --Şirket Kodu
--	BELNR = belnr ,
--	BUZEI = buzei,
--	GJAHR = gjahr,
--	BLART = '',
--	BUDAT = CAST(budat AS nvarchar),
--	BLDAT = CAST(bldat AS nvarchar),
--	CPUDT = '',
--	MAINACCOUNT = main_account, -- Alacak Türü
--	KUNNR = kunnr, -- Müşteri Kodu
--	CUSTOMER = customer , -- Müşteri Adı
--	RBUSA = rbusa , --İş Alanı
--	PROJECTNAME = project_name , -- Proje Adı
--	RTCUR = rtcur , --Döviz Cinsi
--	WSL = wsl,
--	HSL = '',
--	DUEDATE = CAST(CONCAT(LEFT(due_date,4),'-',RIGHT(LEFT(due_date,6),2),'-',LEFT(due_date,2)) AS DATE),
--	DUE_DAYS = DATEDIFF(DAY,CAST(GETDATE() AS DATE),
--				CAST(CONCAT(LEFT(due_date,4),'-',RIGHT(LEFT(due_date,6),2),'-',LEFT(due_date,2)) AS DATE))
--	/**
--		Aktif Pasif projelerin filtrelenmesi için aşağıdaki kolon eklenmiştir.
--		Aktif Pasif bilgisi RTI'nın bütçelerinden alındığı için, RTI'ya dahil olmayan
--		tüm projeler için 'ACTIVE' değeri atanmıştır.
--	**/
--	,ISACTIVE = 'ACTIVE'
--	,COMPANY_CODE = ''
--	,COMPANY_NAME = ''
--	--,[SOURCE] = 'SHAREPOINT'
--	,in_out_group_type = 'In Group'
--FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_bnaccountsreceivable') }}

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
	DUE_DAYS =  DATEDIFF(DAY,CAST(GETDATE() AS DATE),CAST(DUEDATE AS DATE))
	/**
		Aktif Pasif projelerin filtrelenmesi için aşağıdaki kolon eklenmiştir.
		Aktif Pasif bilgisi RTI'nın bütçelerinden alındığı için, RTI'ya dahil olmayan
		tüm projeler için 'ACTIVE' değeri atanmıştır.
	**/
	,ISACTIVE = 'ACTIVE'
	,COMPANY_CODE = ''
	,COMPANY_NAME = ''
	--,[SOURCE] = 'SHAREPOINT'
	,in_out_group_type = 'In Group'
FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retaccountsreceivable') }}


UNION ALL


SELECT
	RBUKRS = CASE WHEN RBUKRS = 'HTK' THEN 'NS_HKP02' END , --Şirket Kodu
	BELNR = BELNR ,
	BUZEI  ,
	GJAHR  ,
	BLART = ''  ,
	BUDAT = CAST(FORMAT(CAST(BUDAT AS DATE), 'yyyyMMdd') AS nvarchar),
	BLDAT = CAST(FORMAT(CAST(BLDAT AS DATE), 'yyyyMMdd') AS nvarchar),
	CPUDT = ''  ,
	MAINACCOUNT , -- Alacak Türü
	KUNNR = LIFNR  , -- Müşteri Kodu
	CUSTOMER = VENDOR   , -- Müşteri Adı
	RBUSA = RBUSA , --İş Alanı
	PROJECTNAME  , -- Proje Adı
	RTCUR = RTCUR , --Döviz Cinsi
	WSL  ,
	HSL = ''  ,
	DUEDATE = CAST(DUEDATE AS DATE)  ,
	DUE_DAYS = DATEDIFF(DAY,CAST(GETDATE() AS DATE),CAST(DUEDATE AS DATE))  
	/**
		Aktif Pasif projelerin filtrelenmesi için aşağıdaki kolon eklenmiştir.
		Aktif Pasif bilgisi RTI'nın bütçelerinden alındığı için, RTI'ya dahil olmayan
		tüm projeler için 'ACTIVE' değeri atanmıştır.
	**/
	,ISACTIVE = 'ACTIVE'
	,COMPANY_CODE = ''
	,COMPANY_NAME = ''
	--,[SOURCE] = 'SHAREPOINT'
	,in_out_group_type = 'In Group'
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_hkpaccountsreceivable') }}

UNION ALL

	SELECT
		RBUKRS = 'NS_BLN' , --Şirket Kodu
		BELNR = Voucher ,
		BUZEI = '1'  ,
		GJAHR = YEAR(TransactionDate_NK)  ,
		/** Eren Bey'lerin test etmesi için datamarta iletmiş oldukları veriler eklenmiştir. 
		Ana dashboardda BLART kolonu 'BLN_TEST' olmayacak şekilde filtrelenmiştir.
		Test dashboardunda ise BLART kolonu 'BLN_TEST' olacak şekilde filtrelenmiştir.
		**/
		BLART = ''  , 
		BUDAT = CAST(FORMAT(CAST(TransactionDate_NK AS DATE), 'yyyyMMdd') AS nvarchar),
		BLDAT = CAST(FORMAT(CAST(TransactionDate_NK AS DATE), 'yyyyMMdd') AS nvarchar),
		CPUDT = ''  ,
		MAINACCOUNT = '' , 
		KUNNR = CAST(c.CustomerId AS nvarchar), -- Müşteri Kodu
		CUSTOMER = CASE
						WHEN c.CustomerGroup = 'BIN-P' THEN CONCAT('Individual Buyer ',CAST(c.rn AS nvarchar))
						ELSE CAST(c.CustomerId AS nvarchar)
					END,
		RBUSA = CAST(Project_NK AS nvarchar) , --İş Alanı
		PROJECTNAME = CAST(Project_NK AS nvarchar) , -- Proje Adı
		RTCUR = CurrencyCode , --Döviz Cinsi
		WSL = AmountCurrency - AmountCurrencySettled  ,
		HSL = ''  ,
		DUEDATE = CAST(DueDate AS DATE)  ,
		DUE_DAYS = DATEDIFF(DAY,CAST(GETDATE() AS DATE),CAST(DueDate AS DATE))  
		/**
			Aktif Pasif projelerin filtrelenmesi için aşağıdaki kolon eklenmiştir.
			Aktif Pasif bilgisi RTI'nın bütçelerinden alındığı için, RTI'ya dahil olmayan
			tüm projeler için 'ACTIVE' değeri atanmıştır.
		**/
		,ISACTIVE = 'ACTIVE'
		,COMPANY_CODE = ct.Company_NK
		,COMPANY_NAME = cmp.CompanyName
		,in_out_group_type = 'In Group'
		-- ,[SOURCE] = 'BN_LS'
		FROM [PRDSYNDW-ONDEMAND.SQL.AZURESYNAPSE.NET].[ronesansdwh].[dbo].[fact_CustomerTransactions] ct
			LEFT JOIN dim_bn_customer c ON c.Customer_NK = ct.Customer_NK
			LEFT JOIN [PRDSYNDW-ONDEMAND.SQL.AZURESYNAPSE.NET].[ronesansdwh].[dbo].[dim_Company] cmp ON cmp.Company_NK = ct.Company_NK
		WHERE 1=1
			AND OpenStatus <> 'Closed'
			AND c.CustomerId NOT IN (
									'203356',
									'203389',
									'203390',
									'201658',
									'203392',
									'203669',
									'202491',
									'203372',
									'207905',
									'203498',
									'203055',
									'204417',
									'204223',
									'202662',
									'207900',
									'202517',
									'201317',
									'206274',
									'200705',
									'206461',
									'207334',
									'200081',
									'200983',
									'200137',
									'200115',
									'202517',
									'201864',
									'200428',
									'200255',
									'200262',
									'200042',
									'200016',
									'200321',
									'201317',
									'200751',
									'200529',
									'200490',
									'202662',
									'200053',
									'200033',
									'200057',
									'200862',
									'201875',
									'201877',
									'202511',
									'202494',
									'200868',
									'200550',
									'200080',
									'201547',
									'203501',
									'203502',
									'203504',
									'204596',
									'205612',
									'204595',
									'206415',
									'201782',
									'200516',
									'208190'			
									)

	)

SELECT
	[rls_region]   = kuc.RegionCode
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(RAW_CTE.company  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(RAW_CTE.business_area  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,RAW_CTE.*
	,receivable_category = CASE WHEN due_days<0 THEN 'Overdue' ELSE 'Outstanding' END
	,CASE
		WHEN ABS(due_days) <=30 THEN '0-30 Days'
		WHEN ABS(due_days) <=60 THEN '31-60 Days'
		WHEN ABS(due_days) <=90 THEN '61-90 Days'
		WHEN ABS(due_days) <=180 THEN '91-180 Days'
		WHEN ABS(due_days) <=365 THEN '181-365 Days'
		ELSE '>365 Days'
	END as due_category
	,transaction_type = 'Account Receivable'
	,turnover_ratio = ar_tr.TURNOVER_RATIO
FROM RAW_CTE
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON RAW_CTE.company = kuc.RobiKisaKod 
	LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_accountsreceivableturnover') }} ar_tr ON 
								RAW_CTE.company = ar_tr.RBUKRS
							AND RAW_CTE.business_area = ar_tr.RBUSA
							AND RAW_CTE.customer_code = ar_tr.KUNNR
