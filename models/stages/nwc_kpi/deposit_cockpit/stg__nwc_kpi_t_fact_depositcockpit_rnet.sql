{{
  config(
    materialized = 'table',tags = ['fi_kpi','deposit_cockpit']
    )
}}


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
		,account_key = CONCAT(FORMID,ORDERID,lstCompanyDesc,lstPB,cmbBankCode,cmbAccountType,cast(txtBalanceDate as date))
		,account_key_without_date = CONCAT(FORMID,ORDERID,lstCompanyDesc,lstPB,cmbBankCode,cmbAccountType)
		,balance_date = cast(txtBalanceDate as date)
		,snapshot_date = cast(snapshot_date as date)
	FROM {{ source('stg_fi_kpi', 'raw__nwc_kpi_t_fact_rnetbankbalances') }}
)

, pivotted_cte AS (

	SELECT
		account_key
		,balance = MAX(txt_balance)
		,txt_rate = MAX(txt_rate)
		,last_snapshot = MAX(snapshot_date)
		,[start_date] = CASE 
							WHEN MIN(txt_maturity_start_date) IS NOT NULL THEN MIN(txt_maturity_start_date) 
							WHEN MAX(balance_date) IS NOT NULL THEN MAX(balance_date) 
							ELSE MAX(snapshot_date) 
						END
		--,end_date = CASE WHEN MAX(txt_due_date) IS NULL THEN MAX(snapshot_date) ELSE MAX(txt_due_date) END
		,end_date = DATEADD(D,1,MAX(snapshot_date))
FROM cte_rnet
GROUP BY account_key
)

,adjusted_end_dates AS (
	/** Vadesiz hesaplar için end date hesaplamasıdır.
		Ek bir hesaplamaya gereksinim duyulmasının sebebi vadesizler için due date bilgisi olmaması ve aşağıdaki örnekte karşılaşılan durumlardır;

		Kayıt 1: start_date (balance_date)'i 01.01.2025 olan bir kayıt 10.01.2025'e kadar snapshot olarak basılıyor
		Kayıt 2: start_date (balance_date)'i 05.01.2025 olan bir kayıt 15.01.2025'e kadar snapshot olarak basılıyor

		Bu iki kaydın aynı banka hesabını temsil etmesi durumunda ve end_date'in max(snapshot_date) olarak alınması durumunda;
		Kayıt 1: 01.01.2025 - 10.01.2025
		Kayıt 2: 05.01.2025 - 15.01.2025 olarak gözüktüğünden, ve bu aralıklardaki tüm günler için bakiye basıldığından,
		05.01.2025 - 09.01.2025 tarihleri aralığında her iki kayıttaki bakiye de veride yansıtılmakta, bu sebeple bu aralıkta veri çoklamakta.

		Aşağıdaki kısım şu formül ile bu sorunu atlatmayı hedefler;
		Aynı banka hesapları için (account_key) kayıtlar max(snapshot_date)'e göre sıralandığında;
		snapshot_date'i üsttekinin start_date'inden büyük olan kayıtlarda, snapshot_date yerine üstteki kaydın start_date'i getirilir.
	
	**/

	SELECT * 
		,end_date_adj = LAG(balance_date) OVER (PARTITION BY account_key_without_date ORDER BY max_snapshot_date DESC, balance_date DESC) 
		,end_date_adj_ir = CASE WHEN  LAG(balance_date) OVER (PARTITION BY account_key_without_date ORDER BY max_snapshot_date DESC, balance_date DESC) IS NULL THEN max_snapshot_date END
	FROM ( 
		SELECT
			account_key_without_date
			,account_key
			,balance = AVG(txt_balance)
			,balance_date
			,max_snapshot_date = MAX(snapshot_date)
		FROM cte_rnet
		GROUP BY 
			account_key_without_date
			,account_key
			,balance_date
		) T

)

,cte_ulke AS (

	SELECT 
		LAND1
		,LANDX
		,NATIO
	FROM {{ ref('vw__s4hana_v_sap_ug_t005t') }}
	WHERE spras = 'T'
	)

,adjusted_rnet AS (

	SELECT
		nonsap = 'X'
		,company 
		,company_description
		,account_number = ''
		,txt20 = ''
		,txt50 = ''
		,cr.amount_transaction_currency 
		,txt_balance
		,pc.txt_rate
		,pc.[start_date]
		-- ,end_date = CASE WHEN pc.txt_rate IS NULL AND aed.end_date_adj IS NOT NULL THEN aed.end_date_adj ELSE pc.end_date END
		,end_date = CASE WHEN aed.end_date_adj IS NULL THEN aed.end_date_adj_ir ELSE aed.end_date_adj END
		,country = TB.ULKE
		,group_ratio = TB.GRUPORANI
		,[group] = TB.GRUP
		,sector = TB.SEKTOR
		,sub_sector = TB.ALTSEKTOR
		,free_restricted_flag = TB.SERBEST
		,bank_country = TULK.LANDX  --ULKE_BANKA
		,cr.bank_name -- AS BANKATANIMI
		,account_type = CASE 
							WHEN cr.account_type = '01' THEN N'Vadesiz'
							WHEN cr.account_type = '02' THEN N'Vadeli'
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
		,pc.account_key
	FROM pivotted_cte pc
		LEFT JOIN cte_rnet cr ON cr.account_key = pc.account_key
							AND cr.snapshot_date = pc.last_snapshot
		LEFT JOIN adjusted_end_dates aed ON aed.account_key = pc.account_key
										AND aed.max_snapshot_date = pc.last_snapshot
		LEFT JOIN {{ source('stg_fi_kpi', 'raw__fi_kpi_t_dim_staticmevduatcompanies') }} TB ON cr.company = TB.bukrs COLLATE Latin1_General_CI_AS
		LEFT JOIN cte_ulke TULK ON cr.bank_country COLLATE Latin1_General_CI_AS = TULK.LAND1
		WHERE 1=1
			AND (TB.NONSAP = 'X' OR TB.NONSAP IS NULL OR TB.NONSAP = '')
			AND (TB.HARICTUT IS NULL OR TB.HARICTUT = '')
)

SELECT 
	nonsap
	,company -- AS BUKRS
	,company_description
	,account_number = account_key
	,txt20
	,txt50
	,amount_transaction_currency
	,txt_balance
	,txt_rate
	,[start_date]
	,end_date
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
					FROM {{ ref('stg__fi_kpi_t_dim_bloombergcurrency') }} bc
					WHERE CONVERT(DATE, LEFT(bc.[Date], 2) + '/' + SUBSTRING(bc.[Date], 3, 2) + '/' + RIGHT(bc.[Date], 4), 104) <= CONVERT(DATE, GETDATE(), 104)
						AND bc.Currency COLLATE Latin1_General_CI_AS = amount_transaction_currency
					ORDER BY CONVERT(DATE, LEFT(bc.[Date], 2) + '/' + SUBSTRING(bc.[Date], 3, 2) + '/' + RIGHT(bc.[Date], 4), 104) DESC
					)
		ELSE txt_balance / (
				SELECT TOP 1 CAST(Value1 AS DECIMAL(18, 2))
				FROM {{ ref('stg__fi_kpi_t_dim_bloombergcurrency') }} bc
				WHERE CONVERT(DATE, LEFT(bc.[Date], 2) + '/' + SUBSTRING(bc.[Date], 3, 2) + '/' + RIGHT(bc.[Date], 4), 104) <= CONVERT(DATE, GETDATE(), 104)
					AND bc.Currency COLLATE Latin1_General_CI_AS = amount_transaction_currency
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
FROM adjusted_rnet ac


