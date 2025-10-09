{{
  config(
    materialized = 'table',tags = ['fi_kpi']
    )
}}


SELECT 
    TARIH = yb.date
    ,NONSAP = 'S'
    ,RBUKRS = yb.[company]
    ,RACCT = CASE WHEN general_ledger_account IS NULL THEN CONCAT('102_',company,'_',deposit_demand_group) ELSE CAST(general_ledger_account AS nvarchar) END
    ,TXT20 = ''
    ,TXT50 = ''
    ,RTCUR = yb.[amount_transaction_currency]
    ,ANLIKBAKIYE_IPB = yb.[balance_ipb]
	,CASE 
		WHEN yb.[amount_transaction_currency] = 'USD'
			THEN yb.[balance_ipb]
		WHEN yb.[amount_transaction_currency] = 'EUR'
			THEN yb.[balance_ipb] * (
					SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
					FROM {{ ref('stg__fi_kpi_t_dim_bloombergcurrency') }} bc
					WHERE bc.Currency = yb.[amount_transaction_currency] COLLATE DATABASE_DEFAULT
						AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, yb.[date], 104)
					ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
					)
		ELSE yb.[balance_ipb] / (
				SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
				FROM {{ ref('stg__fi_kpi_t_dim_bloombergcurrency') }} bc
				WHERE bc.Currency = yb.[amount_transaction_currency] COLLATE DATABASE_DEFAULT
					AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, yb.[date], 104)
				ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
				)
		END AS ANLIKBAKIYE_USD
	,CASE 
		WHEN yb.[amount_transaction_currency] = 'EUR' THEN yb.[balance_ipb]
		WHEN yb.[amount_transaction_currency] = 'USD' THEN yb.[balance_ipb] / (
			SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
			FROM {{ ref('stg__fi_kpi_t_dim_bloombergcurrency') }} bc
			WHERE bc.Currency = 'EUR'
				AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, yb.[date], 104)
			ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
			)
		ELSE 
			(yb.[balance_ipb] / (
			SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
				FROM {{ ref('stg__fi_kpi_t_dim_bloombergcurrency') }} bc
			WHERE bc.Currency = yb.[amount_transaction_currency] COLLATE DATABASE_DEFAULT
				AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, yb.[date], 104)
			ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
			)) / (
				SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
				FROM {{ ref('stg__fi_kpi_t_dim_bloombergcurrency') }} bc
				WHERE bc.Currency = 'EUR'
					AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, yb.[date], 104)
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
    ,ULKE_BANKA = yb.[is_foreign]
    ,BANKATANIMI = yb.[bank_name]
    ,HESAP_TIPI_TANIMI = yb.account_type
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
    ,yb.deposit_demand_group
FROM {{ ref('stg__nwc_kpi_t_fact_ivfaccounts') }} as yb
	LEFT JOIN "aws_stage"."fi_kpi"."raw__fi_kpi_t_dim_staticmevduatcompanies" static_comp ON static_comp.BUKRS = yb.company
	WHERE yb.[date]<=GETDATE()