{{
  config(
    materialized = 'table',tags = ['nwc_kpi','deposit_cockpit']
    )
}}


WITH deposit_cockpit_raw AS (
	/** Vadeli kokpitten gelen vadelilerin bulunduğu veri **/

	/** Kaynak tablodaki statu kolonu karşılıkları
	Talep Durumu	Kısa tanım
	4	2. Yetkili Onayladı
	5	Vadesiz -> Vadeli Kayıt Oluşturuldu
	6	Süreç Tamamlandı
	7	Tahsilat Yapıldı
	8	Süpürme Yapıldı  
	**/

	SELECT
		bukrs 
		,waers 
		,hkont2
		,irate1 
		,wrbtr 
		,wrbtr_eur = wrbtr * c.eur_value
		,calc_wrbtr
		,calc_wrbtr_EUR = calc_wrbtr * c.eur_value
		,expstr 
		,expfin
		,date_diff = DATEDIFF(D,CAST(expstr AS DATE),CAST(expfin AS DATE)) 
		,date_interval = CASE
							WHEN DATEDIFF(D,CAST(expstr AS DATE),CAST(expfin AS DATE)) <= 30 THEN '0 - 30'
							WHEN DATEDIFF(D,CAST(expstr AS DATE),CAST(expfin AS DATE)) > 30 AND DATEDIFF(D,CAST(expstr AS DATE),CAST(expfin AS DATE)) <= 45 THEN '31 - 45'
							WHEN DATEDIFF(D,CAST(expstr AS DATE),CAST(expfin AS DATE)) > 45 AND DATEDIFF(D,CAST(expstr AS DATE),CAST(expfin AS DATE)) <= 90 THEN '46 - 90'
							WHEN DATEDIFF(D,CAST(expstr AS DATE),CAST(expfin AS DATE)) > 90 AND DATEDIFF(D,CAST(expstr AS DATE),CAST(expfin AS DATE)) <= 180 THEN '91 - 180'
							WHEN DATEDIFF(D,CAST(expstr AS DATE),CAST(expfin AS DATE)) > 180 THEN '> 181'
						END
		,b.bank_name
		,c.eur_value
		,is_foreign = CASE
						WHEN b.bank_name = N'GARANTI BANK INTERNATIONAL' THEN N'Yurt Dışı'
						WHEN is_foreign = N'Yurd Dışı' THEN N'Yurt Dışı'
						WHEN is_foreign = N'Yurt içi' THEN N'Yurt İçi'
						ELSE is_foreign
					END
		,vade_durumu = 'Vadeli'
		,source = 'Vadeli Kokpit'
		,deposit_demand_group = CASE 
									WHEN LEFT(hkont2,3) = '118' THEN 'Fund' 
									WHEN LEFT(hkont2,3) IN ('111','112') THEN 'Bond' 
									ELSE 'Deposit' 
								END
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_yfin_vdl_t_001') }} vk
		LEFT JOIN {{ source('stg_fi_kpi', 'raw__fi_kpi_t_dim_banks') }} b ON b.bank_code = SUBSTRING(vk.hkont2,5,3)
		LEFT JOIN {{ ref('stg__dimensions_t_dim_dailys4currencies') }} c ON c.date_value = vk.expstr
																				AND c.currency = vk.waers 
	WHERE 1=1
		AND (expstr <> '0000-00-00' OR expstr IS NULL)
		AND statu IN ('05','06','07')
)

/** EBA'dan gelen vadeli ve vadesizlerin bulunduğu veri **/

	SELECT 
		company = company COLLATE SQL_Latin1_General_CP1_CI_AS
		,currency = amount_transaction_currency COLLATE SQL_Latin1_General_CP1_CI_AS
		,racct = account_number COLLATE SQL_Latin1_General_CP1_CI_AS
		,txtRate = txt_rate
		,txtMarketRate = '0.00'
		,txtBalance = txt_balance
		,txtBalanceEur = txt_balance  * c.eur_value
		,calculated_balance = txt_balance  * c.eur_value * txt_rate * DATEDIFF(D,CAST([start_date] AS DATE),CAST(end_date AS DATE)) / 36500
		,calculated_balance_eur = txt_balance * txt_rate * DATEDIFF(D,CAST([start_date] AS DATE),CAST(end_date AS DATE)) / 36500
		,start_date = CAST([start_date] AS DATE) 
		,end_date = CAST(end_date AS DATE) 
		,date_diff = DATEDIFF(D,CAST([start_date] AS DATE),CAST(end_date AS DATE))
		,date_interval = CASE
							WHEN DATEDIFF(D,CAST([start_date] AS DATE),CAST(end_date AS DATE)) <= 30 THEN '0 - 30'
							WHEN DATEDIFF(D,CAST([start_date] AS DATE),CAST(end_date AS DATE)) > 30 AND DATEDIFF(D,CAST([start_date] AS DATE),CAST(end_date AS DATE)) <= 45 THEN '31 - 45'
							WHEN DATEDIFF(D,CAST([start_date] AS DATE),CAST(end_date AS DATE)) > 45 AND DATEDIFF(D,CAST([start_date] AS DATE),CAST(end_date AS DATE)) <= 90 THEN '46 - 90'
							WHEN DATEDIFF(D,CAST([start_date] AS DATE),CAST(end_date AS DATE)) > 90 AND DATEDIFF(D,CAST([start_date] AS DATE),CAST(end_date AS DATE)) <= 180 THEN '91 - 180'
							WHEN DATEDIFF(D,CAST([start_date] AS DATE),CAST(end_date AS DATE)) > 180 THEN '> 181'
						END
		,bank = bank_name COLLATE SQL_Latin1_General_CP1_CI_AS
		,c.eur_value
		,is_foreign = CASE
						WHEN bank_name = N'GARANTI BANK INTERNATIONAL' THEN N'Yurt Dışı' 
						WHEN bank_country = N'Türkiye' THEN N'Yurt İçi'
						ELSE N'Yurt Dışı'
					END
		,vade_durumu = CASE 
							WHEN bank_name = N'GARANTI BANK INTERNATIONAL' THEN N'Vadeli'
							WHEN bank_name = N'DEUTSCHE BANK' THEN N'Vadeli' 
							WHEN (account_type = N'Vadeli' OR account_type = N'Yuvam') THEN N'Vadeli'
							ELSE account_type END
		,source = 'EBA'
		,deposit_demand_group = CASE 
									WHEN bank_name = N'GARANTI BANK INTERNATIONAL' THEN N'Deposit'
									WHEN bank_name = N'DEUTSCHE BANK' THEN N'Deposit' 
									WHEN (account_type = N'Vadeli' OR account_type = N'Yuvam') THEN N'Deposit'
								ELSE 'Demand' END
	FROM {{ ref('stg__nwc_kpi_t_fact_depositcockpit_rnet') }} r
		LEFT JOIN {{ ref('stg__dimensions_t_dim_dailys4currencies') }} c ON c.date_value = CAST(r.[start_date] AS DATE)
																				AND c.currency = r.amount_transaction_currency COLLATE SQL_Latin1_General_CP1_CI_AS
	WHERE txt_rate IS NOT NULL AND txt_balance IS NOT NULL

UNION ALL

/** Vadeli kokpitten gelen vadelilerin bulunduğu veri **/

select 
	d.bukrs 
	,d.waers 
	,d.hkont2
	,d.irate1 
	,txt_market_rate = CASE 
							WHEN d.date_interval = '0 - 30' THEN f.rate1
							WHEN d.date_interval = '31 - 45' THEN f.rate2
							WHEN d.date_interval = '46 - 90' THEN f.rate3
							WHEN d.date_interval = '91 - 180' THEN f.rate4
							WHEN d.date_interval = '> 181' THEN f.rate5
					   END
	,d.wrbtr 
	,d.wrbtr_eur
	,d.calc_wrbtr
	,d.calc_wrbtr_EUR
	,d.expstr 
	,d.expfin
	,d.date_diff
	,d.date_interval
	,d.bank_name
	,d.eur_value
	,d.is_foreign
	,d.vade_durumu
	,d.source
	,d.deposit_demand_group
from deposit_cockpit_raw d
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_yfin_vdl_t_012') }} f
		ON f.datum = (
			SELECT MAX(datum) 
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_yfin_vdl_t_012') }} f 
			WHERE f.datum <= d.expstr
		)
		AND f.waers = d.waers

-- UNION ALL

/** IVF Sharepointten s4mevduat'a da aktığından bu kısmı kapattım, s4mevduat union'undan rapora akacak **/ 



-- SELECT 
-- 		company
-- 		,currency = amount_transaction_currency
-- 		,racct = CASE WHEN general_ledger_account IS NULL THEN CONCAT('102_',company,'_',deposit_demand_group) ELSE CAST(general_ledger_account AS nvarchar) END 
-- 		,txtRate = interest_rate -- cast(interest_rate as nvarchar)
-- 		,txtMarketRate = '0.00'
-- 		,txtBalance = cast(balance_ipb as money)
-- 		,txtBalanceEur = cast(balance_ipb as money)  * c.eur_value
-- 		,calculated_balance = cast(balance_ipb as money)  * c.eur_value * coalesce(interest_rate,1) * DATEDIFF(D,CAST([start_date] AS DATE),CAST([end_date] AS DATE)) / 36500
-- 		,calculated_balance_eur = cast(balance_ipb as money) * coalesce(interest_rate,1) * DATEDIFF(D,CAST([start_date] AS DATE),CAST([end_date] AS DATE)) / 36500
-- 		,start_date = CAST([start_date] AS DATE) 
-- 		,end_date = CAST(GETDATE() AS DATE) --CAST([end_date] AS DATE) 
-- 		,date_diff = DATEDIFF(D,CAST([start_date] AS DATE),CAST([end_date] AS DATE))
-- 		,date_interval = CASE
-- 							WHEN DATEDIFF(D,CAST([start_date] AS DATE),CAST([end_date] AS DATE)) <= 30 THEN '0 - 30'
-- 							WHEN DATEDIFF(D,CAST([start_date] AS DATE),CAST([end_date] AS DATE)) > 30 AND DATEDIFF(D,CAST([start_date] AS DATE),CAST([end_date] AS DATE)) <= 45 THEN '31 - 45'
-- 							WHEN DATEDIFF(D,CAST([start_date] AS DATE),CAST([end_date] AS DATE)) > 45 AND DATEDIFF(D,CAST([start_date] AS DATE),CAST([end_date] AS DATE)) <= 90 THEN '46 - 90'
-- 							WHEN DATEDIFF(D,CAST([start_date] AS DATE),CAST([end_date] AS DATE)) > 90 AND DATEDIFF(D,CAST([start_date] AS DATE),CAST([end_date] AS DATE)) <= 180 THEN '91 - 180'
-- 							WHEN DATEDIFF(D,CAST([start_date] AS DATE),CAST([end_date] AS DATE)) > 180 THEN '> 181'
-- 						END
-- 		,bank = bank_name
-- 		,c.eur_value
-- 		,is_foreign
-- 		,vade_durumu = account_type
-- 		,source = 'IVF - Sharepoint'
-- 		,deposit_demand_group
-- from {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_ivffundbalances') }} i
-- 		LEFT JOIN {{ ref('stg__dimensions_t_dim_dailys4currencies') }} c ON c.date_value = CAST(i.[start_date] AS DATE)
-- 																				AND c.currency = i.amount_transaction_currency
-- WHERE 1=1
-- 	AND i.account_type = N'Vadeli'


-- UNION ALL


-- /** s4.tb049MevduatBelgeTarihineGöre_BLDAT tablosundan gelen 2024 Şubat öncesinin bulunduğu veriler**/

-- 	SELECT 
-- 		company = RBUKRS 
-- 		,currency = RTCUR
-- 		,RACCT 
-- 		,txtRate = '0.00'
-- 		,txtMarketRate = '0.00'
-- 		,txtBalance = ANLIKBAKIYE_IPB
-- 		,txtBalanceEur = ANLIKBAKIYE_EUR
-- 		,calculated_balance = '0.00'
-- 		,calculated_balance_eur = '0.00'
-- 		,start_date = CAST(v.TARIH AS DATE) 
-- 		,end_date = ''
-- 		,date_diff = ''
-- 		,date_interval = null
-- 		,bank = BANKATANIMI COLLATE DATABASE_DEFAULT
-- 		,c.eur_value
-- 		,is_foreign = CASE
-- 						WHEN ULKE_BANKA = N'Yurd Dışı' THEN N'Yurt Dışı'
-- 						WHEN ULKE_BANKA = N'Yurt içi' THEN N'Yurt İçi'
-- 						ELSE ULKE_BANKA COLLATE DATABASE_DEFAULT
-- 					END
-- 		,vade_durumu = CASE 
-- 							WHEN BANKATANIMI = N'GARANTI BANK INTERNATIONAL' THEN N'Vadeli'  
-- 							WHEN BANKATANIMI = N'DEUTSCHE BANK' THEN N'Vadeli' 
-- 							ELSE HESAP_TIPI_TANIMI COLLATE DATABASE_DEFAULT
-- 					END
-- 		,source = 'MevduatBelgeTarihineGöre'
-- 		,deposit_demand_group = CASE WHEN HESAP_TIPI_TANIMI IN ('Vadeli','Teminat Vadeli','KKM') THEN 'Deposit' ELSE 'Demand' END
-- 	FROM {{ ref('dm__nwc_kpi_t_fact_s4mevduat') }} v
-- 		LEFT JOIN {{ ref('stg__dimensions_t_dim_dailys4currencies') }} c ON c.date_value = CAST(v.TARIH AS DATE)
-- 																			AND c.currency = v.RTCUR
-- 	WHERE 1=1
-- 		AND v.TARIH < '2024-02-01 00:00:00.000'


UNION ALL


/** s4bankbalances tablosundan gelen 2024 Şubat ve sonrasının bulunduğu deposit cockpitte olmayan veriler
	s4bankbalances'da ve depositcockpit'te RNET verileri çakıştığından bu kısımda RNET alınmayacak şekilde filtrelenmiştir **/

 	SELECT 
 		company
 		,amount_transaction_currency
 		,general_ledger_account
 		,txtRate = '0.00'
 		,txtMarketRate = '0.00'
 		,txtBalance = balance_ipb
 		,txtBalanceEur = balance_eur
 		,calculated_balance = '0.00'
 		,calculated_balance_eur = '0.00'
 		,start_date = CAST(v.date AS DATE) 
 		,end_date = DATEADD(D,1,CAST(v.date AS DATE))
 		,date_diff = ''
 		,date_interval = null
 		,bank_name
 		,c.eur_value
 		,is_foreign = CASE
 						WHEN country_bank = N'Yurd Dışı' THEN N'Yurt Dışı'
 						WHEN country_bank = N'Yurt içi' THEN N'Yurt İçi'
 						ELSE country_bank 
 					END
 		,vade_durumu = 'Vadesiz'
 		,source = 'Mevduat'
 		,deposit_demand_group
 	FROM {{ ref('dm__nwc_kpi_t_fact_s4bankbalances') }} v
 		LEFT JOIN {{ ref('stg__dimensions_t_dim_dailys4currencies') }} c ON c.date_value = CAST(v.date AS DATE)
 																			AND c.currency = v.amount_transaction_currency
 	WHERE 1=1
 		AND v.date >= '2024-02-01 00:00:00.000'
 		AND v.deposit_demand_group <> 'Deposit'
		AND v.nonsap <> 'RNET'





