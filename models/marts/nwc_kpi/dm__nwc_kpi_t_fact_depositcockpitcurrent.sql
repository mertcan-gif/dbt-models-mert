
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','deposit_cockpit_current']
    )
}}

WITH account_limits AS (
 
	SELECT *, ROW_NUMBER() OVER(PARTITION BY general_ledger_account ORDER BY account_no) RN
	FROM aws_stage.sharepoint.raw__nwc_kpi_t_dim_account_limit
	WHERE general_ledger_account IS NOT NULL
)

SELECT 
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,company
	,[group] = KyribaGrup
	,reporting_date = CAST(GETDATE() AS DATE) --RMORE'da bunu cast date nasıl yaparız
	,general_ledger_account = racct
	,interest_rate = CASE WHEN CAST(txtRate AS MONEY) = '0' THEN NULL ELSE CAST(txtRate AS MONEY) END --0LARI BOŞ BAS
	,market_rate = CAST(txtMarketRate AS MONEY)
	,interest_rate_normalised = CAST(txt_rate_normal AS MONEY)
	,market_rate_normalised = CAST(txt_market_rate_normal AS MONEY)
	,balance_ipb_total = txtBalance
	,balance_eur_total = txtBalanceEur
	,balance_usd_total = txtBalance * c.usd_value
	,balance_try = CASE WHEN dc.currency = 'TRY' THEN txtBalance ELSE '0' END
	,balance_usd = CASE WHEN dc.currency = 'USD' THEN txtBalance ELSE '0' END
	,balance_eur = CASE WHEN dc.currency = 'EUR' THEN txtBalance ELSE '0' END
	,balance_gbp = CASE WHEN dc.currency = 'GBP' THEN txtBalance ELSE '0' END
	,calculated_balance
	,calculated_balance_eur
	,calculated_balance_usd = calculated_balance * c.usd_value
	,dc.currency
	,start_date
	,end_date = CASE WHEN CAST(end_date AS DATE) = '1900-01-01' THEN NULL ELSE end_date END
	,date_diff
	,date_interval
	,bank
	,is_foreign
	,account_status = vade_durumu -- RMORE'a kolon ekleme geldiğinde currency ayrı kolon olarak eklenecek
	,deposit_demand_group
	,bank_group = CASE 
					WHEN company = 'REC' AND bank LIKE '%IVF%' THEN 'REC - IVF'
					WHEN company = 'REC' AND (bank NOT LIKE '%IVF%' OR bank IS NULL) THEN 'REC - Diğer'
					WHEN company = 'RMI' AND bank LIKE '%IVF%' THEN 'RMI - IVF'
					WHEN company = 'RMI' AND (bank NOT LIKE '%IVF%' OR bank IS NULL) THEN 'RMI - Diğer'
					ELSE 'Other'
				 END
	,ac.limit
FROM {{ ref('dm__nwc_kpi_t_fact_depositcockpit') }} dc
		LEFT JOIN {{ ref('stg__dimensions_t_dim_dailys4currencies') }} c ON c.date_value = dc.start_date
																				AND c.currency = dc.currency 
		LEFT JOIN account_limits ac ON dc.racct = ac.general_ledger_account AND ac.RN = '1'
WHERE 1=1
	AND (([start_date] <= CAST(GETDATE() AS DATE) AND end_date >= CAST(GETDATE() AS DATE))
			OR
		 [start_date] = CAST(GETDATE() AS DATE)
		)


/** Artık deposit cockpit verisinde de vadesizler bulunduğundan s4bankbalances'ı unionlamaya gerek kalmadı **/

-- UNION ALL

-- SELECT 
-- 	rls_region
-- 	,rls_group
-- 	,rls_company
-- 	,rls_businessarea
-- 	,company 
-- 	,KyribaGrup
-- 	,reporting_date = CAST(GETDATE() AS DATE) 
-- 	,general_ledger_account 
-- 	,txtRate = '0.00'
-- 	,txtMarketRate = '0.00'
-- 	,interest_rate_normalised = '0.00'
-- 	,market_rate_normalised = '0.00'
-- 	,balance_ipb
-- 	,balance_eur
-- 	,balance_usd = balance_usd
-- 	,balance_try = CASE WHEN v.amount_transaction_currency = 'TRY' THEN balance_ipb ELSE '0' END
-- 	,balance_usd = CASE WHEN v.amount_transaction_currency = 'USD' THEN balance_ipb ELSE '0' END
-- 	,balance_eur = CASE WHEN v.amount_transaction_currency = 'EUR' THEN balance_ipb ELSE '0' END
-- 	,balance_gbp = CASE WHEN v.amount_transaction_currency = 'GBP' THEN balance_ipb ELSE '0' END
-- 	,calculated_balance = '0.00'
-- 	,calculated_balance_eur = '0.00'
-- 	,calculated_balance_usd = '0.00'
-- 	,amount_transaction_currency
-- 	,start_date = CAST(v.date AS DATE) 
-- 	,end_date = NULL
-- 	,date_diff = NULL
-- 	,date_interval = NULL
-- 	,bank = bank_name COLLATE DATABASE_DEFAULT
-- 	,is_foreign = CASE
-- 					WHEN country_bank = N'Yurd Dışı' THEN N'Yurt Dışı'
-- 					WHEN country_bank = N'Yurt içi' THEN N'Yurt İçi'
-- 					ELSE country_bank COLLATE DATABASE_DEFAULT
-- 				END
-- 	,vade_durumu = CASE 
-- 						WHEN bank_name = N'GARANTI BANK INTERNATIONAL' THEN N'Vadeli'  
-- 						WHEN bank_name = N'DEUTSCHE BANK' THEN N'Vadeli' 
-- 						ELSE account_type COLLATE DATABASE_DEFAULT
-- 				END
-- 	,deposit_demand_group = CASE WHEN account_type IN ('Vadeli','Teminat Vadeli','KKM') THEN 'Deposit' ELSE 'Demand' END
-- 	,bank_group = CASE 
-- 				WHEN company = 'REC' AND bank_name LIKE '%IVF%' THEN 'REC - IVF'
-- 				WHEN company = 'REC' AND (bank_name NOT LIKE '%IVF%' OR bank_name IS NULL) THEN 'REC - Diğer'
-- 				WHEN company = 'RMI' AND bank_name LIKE '%IVF%' THEN 'RMI - IVF'
-- 				WHEN company = 'RMI' AND (bank_name NOT LIKE '%IVF%' OR bank_name IS NULL) THEN 'RMI - Diğer'
-- 				ELSE 'Other'
-- 				END
-- FROM {{ ref('dm__nwc_kpi_t_fact_s4bankbalances') }} v
-- WHERE 1=1
-- 	AND CAST(v.date AS DATE)  = CAST(GETDATE() AS DATE)