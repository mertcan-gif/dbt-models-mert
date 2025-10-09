{{
  config(
    materialized = 'table',tags = ['fi_kpi']
    )
}}

SELECT 
    TARIH = rn.date
    ,NONSAP = 'X'
    ,RBUKRS = rn.[company]
    ,RACCT = account_number
    ,TXT20 = ''
    ,TXT50 = ''
    ,RTCUR = rn.[amount_transaction_currency]
    ,ANLIKBAKIYE_IPB = txt_balance
	,ANLIKBAKIYE_USD = balance_usd
	,CASE 
		WHEN rn.[amount_transaction_currency] = 'EUR' THEN rn.txt_balance
		WHEN rn.[amount_transaction_currency] = 'USD' THEN rn.txt_balance / (
			SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
			FROM "aws_stage"."fi_kpi"."stg__fi_kpi_t_dim_bloombergcurrency" bc
			WHERE bc.Currency = 'EUR'
				AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, rn.[date], 104)
			ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
			)
		ELSE 
			(rn.txt_balance / (
			SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
				FROM "aws_stage"."fi_kpi"."stg__fi_kpi_t_dim_bloombergcurrency" bc
			WHERE bc.Currency = rn.[amount_transaction_currency] COLLATE DATABASE_DEFAULT
				AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, rn.[date], 104)
			ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
			)) / (
				SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
				FROM "aws_stage"."fi_kpi"."stg__fi_kpi_t_dim_bloombergcurrency" bc
				WHERE bc.Currency = 'EUR'
					AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, rn.[date], 104)
				ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
				)
	END
	AS ANLIKBAKIYE_EUR
    ,ULKE = static_comp.ULKE
    ,GRUPORANI = static_comp.GRUPORANI
    ,GRUP = static_comp.GRUP
    ,SEKTOR = static_comp.SEKTOR
    ,ALTSEKTOR = static_comp.ALTSEKTOR
    ,SERBEST = static_comp.SERBEST
    ,ULKE_BANKA = CASE
						WHEN rn.bank_name = N'GARANTI BANK INTERNATIONAL' THEN N'Yurt Dışı' 
						WHEN rn.bank_country = N'Türkiye' THEN N'Yurt İçi'
						ELSE N'Yurt Dışı'
					END
    ,BANKATANIMI = rn.[bank_name]
    ,HESAP_TIPI_TANIMI = CASE 
							WHEN rn.[bank_name] = N'GARANTI BANK INTERNATIONAL' THEN N'Vadeli'
							WHEN rn.[bank_name] = N'DEUTSCHE BANK' THEN N'Vadeli' 
							WHEN (rn.account_type = N'Vadeli' OR rn.account_type = N'Yuvam') THEN N'Vadeli'
							ELSE rn.account_type END 
    ,KREDIGRUBU = static_comp.KREDIGRUBU
    ,CONTRIBUTEGROUP = static_comp.CONTRIBUTEGROUP
    ,YK_SEKTOR = static_comp.YK_SEKTOR
    ,YK_ULKE = static_comp.YK_ULKE
    ,YK_KREDIGRUBU = static_comp.YK_KREDIGRUBU
    ,KREDIKATEGORISI = static_comp.KREDIKATEGORISI
    ,YK_KREDIKISITI = static_comp.YK_KREDIKISITI
    ,KA_KREDIGRUBU = static_comp.KA_KREDIGRUBU
    ,KA_KREDIKISITI = static_comp.KA_KREDIKISITI
    ,CASH_GRUP1 = static_comp.CASH_GRUP1
    ,CASH_GRUP2 = static_comp.CASH_GRUP2
    ,CASH_GRUP3 = static_comp.CASH_GRUP3
    ,CASH_GRUP4 = static_comp.CASH_GRUP4
    ,deposit_demand_group = CASE 
								WHEN bank_name = N'GARANTI BANK INTERNATIONAL' THEN N'Deposit'
								WHEN bank_name = N'DEUTSCHE BANK' THEN N'Deposit' 
								WHEN (account_type = N'Vadeli' OR account_type = N'Yuvam') THEN N'Deposit'
							ELSE 'Demand' END
FROM {{ ref('stg__nwc_kpi_t_fact_rnetaccounts') }} as rn
	LEFT JOIN {{ source('stg_fi_kpi', 'raw__fi_kpi_t_dim_staticmevduatcompanies') }} static_comp ON static_comp.BUKRS = rn.company COLLATE Latin1_General_CI_AS
	WHERE rn.[date]<=GETDATE()