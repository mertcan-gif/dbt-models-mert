{{
  config(
    materialized = 'table',tags = ['fi_kpi','s4bankbalances']
    )
}}

WITH RAW_DATA AS (
--HANADAN GELEN DATALAR
SELECT mbt.[DATE] AS TARIH
	,mbt.[NONSAP]
	,mbt.[RBUKRS]
	,mbt.[RACCT]
	,mbt.[TXT20]
	,mbt.[TXT50]
	,mbt.[RTCUR]
	,ANLIKBAKIYE_IPB = CASE
							WHEN TCURX.CURRDEC = 3 THEN mbt.[RunningTotal]/10 
						ELSE mbt.[RunningTotal]  END
	,CASE 
		WHEN mbt.[RTCUR] = 'USD'
			THEN mbt.[RunningTotal]
		WHEN mbt.[RTCUR] = 'EUR'
			THEN mbt.[RunningTotal] * (
					SELECT TOP 1 CONVERT(FLOAT, bc.Value1)

					

					FROM [RNSBI].[RNSBI].[dbo].[tb146BloombergCurrency] bc
					WHERE bc.Currency = mbt.RTCUR COLLATE DATABASE_DEFAULT
						AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, mbt.DATE, 104)
					ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
					)
		WHEN TCURX.CURRDEC = 3 
			THEN (mbt.[RunningTotal] / (
					SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
						FROM [RNSBI].[RNSBI].[dbo].[tb146BloombergCurrency] bc
					WHERE bc.Currency = mbt.RTCUR COLLATE DATABASE_DEFAULT
						AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, mbt.DATE, 104)
					ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
					))/10
		ELSE mbt.[RunningTotal] / (
				SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
				FROM [RNSBI].[RNSBI].[dbo].[tb146BloombergCurrency] bc
				WHERE bc.Currency = mbt.RTCUR COLLATE DATABASE_DEFAULT
					AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, mbt.DATE, 104)
				ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
				)
		END AS ANLIKBAKIYE_USD
	,CASE 
		WHEN mbt.[RTCUR] = 'EUR' THEN mbt.[RunningTotal]
		WHEN mbt.[RTCUR] = 'USD' THEN mbt.[RunningTotal] / (
			SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
			FROM [RNSBI].[RNSBI].[dbo].[tb146BloombergCurrency] bc
			WHERE bc.Currency = 'EUR'
				AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, mbt.DATE, 104)
			ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
			)
		WHEN TCURX.CURRDEC = 3 
			THEN ((mbt.[RunningTotal] / (
					SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
					FROM [RNSBI].[RNSBI].[dbo].[tb146BloombergCurrency] bc
					WHERE bc.Currency = mbt.RTCUR COLLATE DATABASE_DEFAULT
						AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, mbt.DATE, 104)
					ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
					)) / (
						SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
						FROM [RNSBI].[RNSBI].[dbo].[tb146BloombergCurrency] bc
						WHERE bc.Currency = 'EUR'
							AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, mbt.DATE, 104)
						ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
						))/10
		ELSE 
			(mbt.[RunningTotal] / (
			SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
				FROM [RNSBI].[RNSBI].[dbo].[tb146BloombergCurrency] bc
			WHERE bc.Currency = mbt.RTCUR COLLATE DATABASE_DEFAULT
				AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, mbt.DATE, 104)
			ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
			)) / (
				SELECT TOP 1 CONVERT(FLOAT, bc.Value1)
				FROM [RNSBI].[RNSBI].[dbo].[tb146BloombergCurrency] bc
				WHERE bc.Currency = 'EUR'
					AND CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) <= CONVERT(DATE, mbt.DATE, 104)
				ORDER BY CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104) DESC
				)
	END
	AS ANLIKBAKIYE_EUR
	,mbt.[ULKE]
	,mbt.[GRUPORANI]
	,mbt.[GRUP]
	,mbt.[SEKTOR]
	,mbt.[ALTSEKTOR]
	,mbt.[SERBEST]
	,mbt.[ULKE_BANKA]
	,(
		CASE 
			WHEN LEFT(mbt.RACCT COLLATE DATABASE_DEFAULT, 3) in ('111','112')
				THEN N'TAHVİL/BONO'
			WHEN (
					mbt.BANKATANIMI IS NULL
					OR mbt.BANKATANIMI = ''
					)
				THEN N'Tanımlı Değil'
			ELSE mbt.BANKATANIMI
			END
		) [BANKATANIMI]
	,mbt.[HESAP_TIPI_TANIMI]
	,mbt.[KREDIGRUBU]
	,mbt.[CONTRIBUTEGROUP]
	,mbt.[YK_SEKTOR]
	,mbt.[YK_ULKE]
	,mbt.[YK_KREDIGRUBU]
	,mbt.[KREDIKATEGORISI]
	,mbt.[YK_KREDIKISITI]
	,mbt.[KA_KREDIGRUBU]
	,mbt.[KA_KREDIKISITI]
	,mbt.[CASH_GRUP1]
	,mbt.[CASH_GRUP2]
	,mbt.[CASH_GRUP3]
	,mbt.[CASH_GRUP4]
FROM {{ ref('stg__nwc_kpi_t_dim_s4mevduatadjusted') }} mbt
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON mbt.RTCUR = TCURX.CURRKEY 
	
WHERE 1=1
	--AND SUBSTRING(mbt.RACCT, 5, 3) <> '538' 
	--AND SUBSTRING(mbt.RACCT, 5, 3) <> '038'
	AND mbt.[DATE] >= '2022-12-31' 
	AND (RACCT <> '1020068013' AND RACCT <> '1021068017' AND RACCT<>'1021068025')
)


SELECT 
	[date] = TARIH
	,nonsap = NONSAP
	,company = TRIM(RBUKRS)
	,general_ledger_account = RACCT
	,txt20 = TXT20
	,txt50 = TXT50
	,amount_transaction_currency = RTCUR
	,balance_ipb = ANLIKBAKIYE_IPB
	,balance_usd = ANLIKBAKIYE_USD
	,balance_eur = ANLIKBAKIYE_EUR
	,country = ULKE
	,group_rate = GRUPORANI 
	,[group] = GRUP
	,sector = SEKTOR
	,sub_sector = ALTSEKTOR
	,free_restricted = SERBEST
	,country_bank = CASE 
					WHEN BANKATANIMI = N'GARANTI BANK INTERNATIONAL' THEN N'Yurt Dışı' 
					WHEN BANKATANIMI = N'GARANTI BANK INTERNATIONAL N.V.' THEN N'Yurt Dışı' 
				  ELSE ULKE_BANKA END
	,bank_name = BANKATANIMI
	,account_type = CASE 
							WHEN BANKATANIMI = N'GARANTI BANK INTERNATIONAL' THEN N'Vadeli' 
							WHEN BANKATANIMI = N'GARANTI BANK INTERNATIONAL N.V.' THEN N'Vadeli' 
							WHEN BANKATANIMI = N'DEUTSCHE BANK' THEN N'Vadeli' 
							WHEN LEFT(RACCT,3) = '118' THEN 'Fund' 
							WHEN LEFT(RACCT,3) IN ('111','112') THEN 'Bond'
						ELSE HESAP_TIPI_TANIMI END
	,credit_group = KREDIGRUBU
	,contribute_group = CONTRIBUTEGROUP
	,yk_sector = YK_SEKTOR
	,yk_country = YK_ULKE
	,yk_credit_group = YK_KREDIGRUBU
	,credit_category = KREDIKATEGORISI
	,yk_credit_constraint = YK_KREDIKISITI
	,ka_credit_group = KA_KREDIGRUBU
	,ka_credit_constraint = KA_KREDIKISITI
	,cash_group_1 = CASH_GRUP1
	,cash_group_2 = CASH_GRUP2
	,cash_group_3 = CASH_GRUP3
	,cash_group_4 = CASH_GRUP4
	,deposit_demand_group = CASE 
								WHEN BANKATANIMI = N'GARANTI BANK INTERNATIONAL' THEN N'Deposit' 
								WHEN BANKATANIMI = N'GARANTI BANK INTERNATIONAL N.V.' THEN N'Deposit' 
								WHEN BANKATANIMI = N'DEUTSCHE BANK' THEN N'Deposit' 
								WHEN LEFT(RACCT,3) = '118' THEN 'Fund' 
								WHEN LEFT(RACCT,3) IN ('111','112') THEN 'Bond'
								WHEN HESAP_TIPI_TANIMI IN ('Vadeli','Teminat Vadeli','KKM') THEN 'Deposit' 
						ELSE 'Demand' END
FROM RAW_DATA

