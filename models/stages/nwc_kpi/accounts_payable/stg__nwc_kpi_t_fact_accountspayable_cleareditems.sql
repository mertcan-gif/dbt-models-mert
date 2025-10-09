
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
		main_account = LEFT(RACCT,3), -- Borç Türü
		vendor_code = LFA1.LIFNR, -- Satıcı Kodu
		vendor = LFA1.NAME1, -- Satıcı Adı
		business_area = ACDOCA.RBUSA, --İş Alanı
		business_area_description= T001W.NAME1, -- Proje Adı
		document_currency = RWCUR, --Döviz Cinsi
		amount_in_document_currency = CASE
				  WHEN TCURX.CURRDEC = 3 THEN WSL/10 
			  ELSE WSL END,
		amount_in_company_currency = HSL,
		due_date = CAST(NETDT AS DATE),
		due_days = DATEDIFF(DAY,CAST(GETDATE() AS DATE),CAST(NETDT AS DATE))
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

 	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} AS ACDOCA
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} AS LFA1 ON ACDOCA.LIFNR = LFA1.LIFNR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} AS T001W ON ACDOCA.RBUSA = T001W.WERKS 
		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} AS BKPF ON
					ACDOCA.BELNR = BKPF.BELNR 
					AND ACDOCA.RBUKRS = BKPF.BUKRS
					AND ACDOCA.GJAHR = BKPF.GJAHR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} AS TCURX ON ACDOCA.RWCUR = TCURX.CURRKEY 
		LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON ACDOCA.RBUKRS = kuc.RobiKisaKod 
	WHERE 1=1

		AND LEFT(RACCT,3) IN ('320')
		--AND CAST(H_BUDAT AS date) <= '2023-05-31'
		AND LEN(ACDOCA.LIFNR)<>3
		--AND _BKPF.STBLG <> 'X'
		AND ACDOCA.BUZEI <> '000'
		AND ACDOCA.LIFNR <> ''
		--AND ACDOCA.AUGBL = ''
		AND NETDT <> '00000000'

	/*******  Ballast Nedam *******/
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
		MAINACCOUNT = CAST(main_account AS nvarchar), -- Alacak Türü
		LIFNR = CAST(LIFNR  AS nvarchar), -- Müşteri Kodu
		VENDOR = VENDOR , -- Müşteri Adı
		RBUSA = CONCAT('BNAP_',CAST(DENSE_RANK() OVER(ORDER BY project_name) AS NVARCHAR)) , --İş Alanı
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
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_bnaccountspayable') }}
	/*******  Ballast Nedam *******/


	/*******  RET  *******/
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
		MAINACCOUNT = CAST(MAINACCOUNT AS nvarchar), -- Alacak Türü
		LIFNR = CAST(LIFNR  AS nvarchar), -- Müşteri Kodu -- COLLATE'i çıkardım hata veriyordu.
		VENDOR = VENDOR , -- Müşteri Adı
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
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retaccountspayable') }}
	
	
	-- Henüz job'a eklenmedi, eklendiğinde yeni adı ile eklenip burada güncellenecek
	/*******  RET *******/



)
SELECT
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(RAW_CTE.company  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(RAW_CTE.business_area  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,RAW_CTE.*
	,CASE WHEN due_days<0 THEN 'Overdue' ELSE 'Outstanding' END AS payable_category
	,CASE
		WHEN ABS(due_days) <=30 THEN '0-30 Days'
		WHEN ABS(due_days) <=60 THEN '31-60 Days'
		WHEN ABS(due_days) <=90 THEN '61-90 Days'
		WHEN ABS(due_days) <=180 THEN '91-180 Days'
		WHEN ABS(due_days) <=365 THEN '181-365 Days'
		ELSE '>365 Days'
	END as due_category
	,transaction_type = 'Account Payable'
	,turnover_ratio = NULL
	,kyriba_group = kuc.KyribaGrup
	,kyriba_company_code = kuc.KyribaKisaKod
FROM RAW_CTE
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON RAW_CTE.company = kuc.RobiKisaKod 
