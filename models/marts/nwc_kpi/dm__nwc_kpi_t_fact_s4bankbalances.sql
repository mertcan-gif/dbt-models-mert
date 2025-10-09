
{{
  config(
    materialized = 'table',tags = ['fi_kpi','s4bankbalances']
    )
}}


SELECT
	[rls_region] = (select top 1 RegionCode from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE mbt.company = kuc.RobiKisaKod )
	,[rls_group] = 
		CONCAT(
			COALESCE((select top 1 KyribaGrup from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE mbt.company = kuc.RobiKisaKod ),'')
			,'_'
			,COALESCE((select top 1 RegionCode from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE mbt.company = kuc.RobiKisaKod ),'')
			)
	,[rls_company] =
		CONCAT(
			COALESCE(company,'')
			,'_'
			,COALESCE((select top 1 RegionCode from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE mbt.company = kuc.RobiKisaKod ),'')
			)
	,[rls_businessarea] = CONCAT('_',(select top 1 RegionCode from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE mbt.company = kuc.RobiKisaKod ))
	,KyribaGrup = (select top 1 KyribaGrup from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE mbt.company = kuc.RobiKisaKod )
	,KyribaKisaKod = (select top 1 KyribaKisaKod from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE mbt.company = kuc.RobiKisaKod )
	,* 
FROM (
	
	SELECT *
	FROM {{ ref('stg__nwc_kpi_t_fact_s4bankbalances') }}
    WHERE 1=1
		-- AND (general_ledger_account like '102%'  OR general_ledger_account like '111%'  OR general_ledger_account like '118%') 
		AND (general_ledger_account like '102%'  OR general_ledger_account like '111%'  OR general_ledger_account like '118%' OR general_ledger_account like '112%') 
		AND [date] >= '2023-01-01 00:00:00.000'
		/** Banka ilişkileri deposit cockpit sayfasında ZYT'yi görmek istemiyorlar. KyribaUnionCompany'de
		Pasif olarak gözüküyor, fakat tüm Pasif'leri filtreleyince de BNA'yı takip etmek isteyen kişiler onu takip
		edemiyor. İş birimi ile banka ilişkileri farklı şekilde takip ediyorlar **/
		AND company <> 'ZYT'
		
	UNION ALL 


/** 2025-05-30 Öncesi RNET verileri **/
	SELECT
		[date]
		,nonsap = 'RNET'
		,company
		,account_number  = CONCAT('102_',company,bank_name)
		,txt20
		,txt50
		,amount_transaction_currency
		,txt_balance
		,balance_usd
		,balance_eur =  balance_usd /
                             (SELECT TOP (1) Value1
                               FROM            RNSBI.RNSBI.dbo.tb146BloombergCurrency b
                               WHERE        (Currency = 'EUR') AND CONVERT(Date, RIGHT(b.[date], 4) + RIGHT(LEFT(b.[date], 4), 2) + LEFT(b.[date], 2)) <= r.[date] 
                               ORDER BY CONVERT(Date, RIGHT(b.[date], 4) + RIGHT(LEFT(b.[date], 4), 2) + LEFT(b.[date], 2)) DESC)
		,country
		,group_ratio
		,[group]
		,sector
		,sub_sector
		,free_restricted_flag
		,bank_country
		,bank_name
		,account_type
		,credit_group
		,contribute_group
		,yk_sector
		,yk_country
		,yk_credit_group
		,credit_category
		,yk_credit_constraint
		,ka_credit_group
		,ka_credit_constraint
		,cash_group_1
		,cash_group_2
		,cash_group_3
		,cash_group_4
		,deposit_demand_group = CASE 
									WHEN LEFT(account_number,3) = '118' THEN 'Fund' 
									WHEN LEFT(account_number,3) IN ('111','112') THEN 'Bond' 
									WHEN account_type IN (N'Vadeli',N'Teminat Vadeli',N'KKM') THEN 'Deposit' 
									ELSE 'Demand' 
								END
	FROM {{ source('stg_fi_kpi', 'raw__fi_kpi_t_fact_rnetbankbalancesarchive') }} r
	WHERE 1=1
		AND [date] >= '2024-01-01'
		AND [date] < '2025-05-30'

	UNION ALL 

/** 2025-05-30 Sonrası RNET verileri **/
	SELECT  
		[TARIH]
      ,[NONSAP] = 'RNET'
      ,[RBUKRS] COLLATE SQL_Latin1_General_CP1_CI_AS
      ,[RACCT] COLLATE SQL_Latin1_General_CP1_CI_AS
      ,[TXT20]
      ,[TXT50]
      ,[RTCUR] COLLATE SQL_Latin1_General_CP1_CI_AS
      ,[ANLIKBAKIYE_IPB]
      ,[ANLIKBAKIYE_USD]
      ,[ANLIKBAKIYE_EUR]
      ,[ULKE]
      ,[GRUPORANI]
      ,[GRUP]
      ,[SEKTOR]
      ,[ALTSEKTOR]
      ,[SERBEST]
      ,[ULKE_BANKA]
      ,[BANKATANIMI] COLLATE SQL_Latin1_General_CP1_CI_AS
      ,[HESAP_TIPI_TANIMI]
      ,[KREDIGRUBU]
      ,[CONTRIBUTEGROUP]
      ,[YK_SEKTOR]
      ,[YK_ULKE]
      ,[YK_KREDIGRUBU]
      ,[KREDIKATEGORISI]
      ,[YK_KREDIKISITI]
      ,[KA_KREDIGRUBU]
      ,[KA_KREDIKISITI]
      ,[CASH_GRUP1]
      ,[CASH_GRUP2]
      ,[CASH_GRUP3]
      ,[CASH_GRUP4]
      ,[deposit_demand_group]
  	FROM {{ ref('stg__nwc_kpi_t_fact_rnetbankbalances') }}

	UNION ALL 

	SELECT  
		[TARIH]
      ,[NONSAP] = 'YUVAM'
      ,[RBUKRS]
      ,[RACCT]
      ,[TXT20]
      ,[TXT50]
      ,[RTCUR]
      ,[ANLIKBAKIYE_IPB]
      ,[ANLIKBAKIYE_USD]
      ,[ANLIKBAKIYE_EUR]
      ,[ULKE]
      ,[GRUPORANI]
      ,[GRUP]
      ,[SEKTOR]
      ,[ALTSEKTOR]
      ,[SERBEST]
      ,[ULKE_BANKA]
      ,[BANKATANIMI]
      ,[HESAP_TIPI_TANIMI]
      ,[KREDIGRUBU]
      ,[CONTRIBUTEGROUP]
      ,[YK_SEKTOR]
      ,[YK_ULKE]
      ,[YK_KREDIGRUBU]
      ,[KREDIKATEGORISI]
      ,[YK_KREDIKISITI]
      ,[KA_KREDIGRUBU]
      ,[KA_KREDIKISITI]
      ,[CASH_GRUP1]
      ,[CASH_GRUP2]
      ,[CASH_GRUP3]
      ,[CASH_GRUP4]
      ,[deposit_demand_group]
  FROM {{ ref('stg__nwc_kpi_t_fact_yuvammevduat') }}

  UNION ALL

	SELECT  
		[TARIH]
      ,[NONSAP] = 'IVF'
      ,[RBUKRS]
      ,[RACCT]
      ,[TXT20]
      ,[TXT50]
      ,[RTCUR]
      ,[ANLIKBAKIYE_IPB]
      ,[ANLIKBAKIYE_USD]
      ,[ANLIKBAKIYE_EUR]
      ,[ULKE]
      ,[GRUPORANI]
      ,[GRUP]
      ,[SEKTOR]
      ,[ALTSEKTOR]
      ,[SERBEST]
      ,[ULKE_BANKA]
      ,[BANKATANIMI]
      ,[HESAP_TIPI_TANIMI]
      ,[KREDIGRUBU]
      ,[CONTRIBUTEGROUP]
      ,[YK_SEKTOR]
      ,[YK_ULKE]
      ,[YK_KREDIGRUBU]
      ,[KREDIKATEGORISI]
      ,[YK_KREDIKISITI]
      ,[KA_KREDIGRUBU]
      ,[KA_KREDIKISITI]
      ,[CASH_GRUP1]
      ,[CASH_GRUP2]
      ,[CASH_GRUP3]
      ,[CASH_GRUP4]
      ,[deposit_demand_group]
  FROM {{ ref('stg__nwc_kpi_t_fact_ivfmevduat') }}
) mbt