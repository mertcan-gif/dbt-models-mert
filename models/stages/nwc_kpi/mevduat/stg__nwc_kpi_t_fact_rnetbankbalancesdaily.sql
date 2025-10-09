{{
  config(
    materialized = 'table',tags = ['fi_kpi','s4mevduat','mevduatxyz2']
    )
}}

/** 
	RNET banka bakiyelerinin güncel halini gösteren tablodur. 

	SQL Server Agent'ta "jb042_daily_rnet_bank_balances_backup" jobunda daily olarak incremental bir şekilde;
		"aws_stage.fi_kpi.raw__fi_kpi_t_fact_rnetbankbalancesdailyarchive"
		tablosuna eklenmektedir
	Tüm verilerin olduğu ana tablo "stg__nwc_kpi_t_fact_rnetbankbalances" tablosudur. Tüm verileri tutan tabloda bir sorun yaşandığı durumda;
		"aws_stage.fi_kpi.raw__fi_kpi_t_fact_rnetbankbalancesdailyarchive" tablosu ile;
		"aws_stage.fi_kpi.raw__fi_kpi_t_fact_rnetbankbalancesarchive" tablosu union edilirse;
		"stg__nwc_kpi_t_fact_rnetbankbalances" tablosunun son haline ulaşılabilir. 
**/

WITH cte_rnet AS (
	SELECT 
		company = lstCompanyDesc --BUKRS
		,company_description = lstCompanyDesc_TEXT --BUTXT
		,bank_country = cmbBankCountry --LAND1
		,bank_code = cmbBankCode --KOD
		,bank_name = cmbBankCode_TEXT --TANIM
		,[control] = '01' --AS KNTRL
		,account_type = CASE 
			WHEN cmbAccountType = '1' THEN '02'
			WHEN cmbAccountType = '2' THEN '01'
		 END  --HSTIP
		,amount_transaction_currency = lstPB  --RTCUR
		,txt_balance = txtBalance --DMBE3
		,txt_rate = txtRate
		,txt_maturity_start_date = txtMaturityStartDate 
		,txt_due_date = txtDueDate
	FROM EBA.EBA.dbo.E_Z168Nonsapdata_frmNonSAPData_dtlData
)

,cte_ulke AS (

	SELECT 
		LAND1
		,LANDX
		,NATIO
	FROM {{ ref('vw__s4hana_v_sap_ug_t005t') }}
	WHERE spras = 'T'
	)
	
,adjusted_cte AS (

	SELECT 
		nonsap = 'X'
		,company 
		,company_description
		,account_number = ''
		,txt20 = ''
		,txt50 = ''
		,amount_transaction_currency  = TN.amount_transaction_currency COLLATE SQL_Latin1_General_CP850_BIN2
		,txt_balance -- AS ANLIKBAKIYE_IPB
		,txt_rate
		,txt_maturity_start_date
		,txt_due_date
		,country = TB.ULKE
		,group_ratio = TB.GRUPORANI
		,[group] = TB.GRUP
		,sector = TB.SEKTOR
		,sub_sector = TB.ALTSEKTOR
		,free_restricted_flag = TB.SERBEST
		,bank_country = TULK.LANDX  --ULKE_BANKA
		,TN.bank_name -- AS BANKATANIMI
		,account_type = CASE 
							WHEN TN.account_type = '01' THEN N'Vadesiz'
							WHEN TN.account_type = '02' THEN N'Vadeli'
							ELSE N'Tanımlı Değil'
						END
		,credit_group = TB.KREDIGRUBU
		,contribute_group = TB.CONTRIBUTEGROUP
		,yk_country = TB.YK_ULKE
		,yk_sector = TB.YK_SEKTOR
		,credit_category = TB.KREDIKATEGORISI
		,yk_credit_group = TB.YK_KREDIGRUBU
		,yk_credit_constraint = TB.YK_KREDIKISITI
		,ka_credit_group = TB.KA_KREDIGRUBU
		,ka_credit_constraint = TB.KA_KREDIKISITI
		,cash_group_1 = TB.CASH_GRUP1
		,cash_group_2 = TB.CASH_GRUP2
		,cash_group_3 = TB.CASH_GRUP3
		,cash_group_4 = TB.CASH_GRUP4
	FROM cte_rnet AS TN WITH (NOLOCK)
	LEFT JOIN {{ source('stg_fi_kpi', 'raw__fi_kpi_t_dim_staticmevduatcompanies') }} AS TB WITH (NOLOCK) ON TN.company = TB.bukrs COLLATE Latin1_General_CI_AS
	LEFT JOIN cte_ulke AS TULK WITH (NOLOCK) ON TN.bank_country COLLATE Latin1_General_CI_AS = TULK.LAND1
	WHERE 1=1
		AND (TB.NONSAP = 'X' OR TB.NONSAP IS NULL OR TB.NONSAP = '')
		AND (TB.HARICTUT IS NULL OR TB.HARICTUT = '')
)

SELECT 
	nonsap
	,company -- AS BUKRS
	,company_description
	,account_number
	,txt20
	,txt50
	,amount_transaction_currency
	,txt_balance
	,txt_rate
	,txt_maturity_start_date
	,txt_due_date
	,country
	,group_ratio
	,[group]
	,sector
	,sub_sector
	,free_restricted_flag
	,CASE 
		WHEN amount_transaction_currency = 'USD'
			THEN txt_balance
		WHEN amount_transaction_currency = 'EUR'
			THEN txt_balance * (
					SELECT TOP 1 CAST(Value1 AS DECIMAL(18, 2))
					FROM RNSBI.RNSBI.dbo.tb146BloombergCurrency bc
					WHERE CONVERT(DATE, LEFT(bc.[Date], 2) + '/' + SUBSTRING(bc.[Date], 3, 2) + '/' + RIGHT(bc.[Date], 4), 104) <= CONVERT(DATE, GETDATE(), 104)
						AND bc.Currency = amount_transaction_currency
					ORDER BY CONVERT(DATE, LEFT(bc.[Date], 2) + '/' + SUBSTRING(bc.[Date], 3, 2) + '/' + RIGHT(bc.[Date], 4), 104) DESC
					)
		ELSE txt_balance / (
				SELECT TOP 1 CAST(Value1 AS DECIMAL(18, 2))
				FROM RNSBI.RNSBI.dbo.tb146BloombergCurrency bc
				WHERE CONVERT(DATE, LEFT(bc.[Date], 2) + '/' + SUBSTRING(bc.[Date], 3, 2) + '/' + RIGHT(bc.[Date], 4), 104) <= CONVERT(DATE, GETDATE(), 104)
					AND bc.Currency = amount_transaction_currency
				ORDER BY CONVERT(DATE, LEFT(bc.[Date], 2) + '/' + SUBSTRING(bc.[Date], 3, 2) + '/' + RIGHT(bc.[Date], 4), 104) DESC
				)
		END AS balance_usd
	,ROUND(group_ratio * txt_balance, 2) AS balance_ipbg
	,1 AS balance_usdgr
	,GETDATE() AS entry_date
	
	,CONVERT(DATE, GETDATE()) AS [date]
	,tomorrow_date = CONVERT(DATE, DATEADD(day, CASE DateDiff(day, '19000101', GETDATE()) % 7
													WHEN 4 THEN 3
													WHEN 5 THEN 2
												ELSE 1 END
											, GETDATE())
							)
	,bank_country = CASE 
						WHEN (bank_country IS NULL OR bank_country = ' ') THEN N'Tanımlı Değil'
						ELSE bank_country
					END
	,bank_name = CASE 
					WHEN (bank_name IS NULL OR bank_name = '') THEN N'Tanımlı Değil'
					ELSE bank_name
				 END
	,account_type =	CASE 
						WHEN (account_type IS NULL OR account_type = ' ') THEN N'Tanımlı Değil'
						ELSE account_type
					END
	,credit_group
	,contribute_group
	,yk_country
	,yk_sector
	,credit_category
	,yk_credit_group
	,yk_credit_constraint = ISNULL(yk_credit_constraint, N'Free')
	,ka_credit_group
	,ka_credit_constraint = ISNULL(ka_credit_constraint, N'Free')
	,cash_group_1
	,cash_group_2
	,cash_group_3
	,cash_group_4	
FROM adjusted_cte ac
