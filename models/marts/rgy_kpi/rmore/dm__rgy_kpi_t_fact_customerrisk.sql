{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

WITH acdoca AS (
SELECT 
	rbukrs,
	kunnr,
	budat,
	racct,
	blart,
	umskz,
	augdt,
	hsl,
	amount = 
		CASE 
			WHEN rtcur = 'EUR' THEN a.tsl * c.ukurs
			WHEN rtcur = 'USD' THEN a.tsl * c.ukurs
			WHEN rtcur = 'TRY' THEN a.tsl
		END
FROM {{ ref('stg__s4hana_t_sap_acdoca') }} a
LEFT JOIN (
	SELECT CONVERT(DATE, gdatu, 104) as _date, fcurr, ukurs
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurr') }}
	WHERE 1=1
	AND kurst = 'M'
	AND fcurr in ('EUR', 'USD')
	AND tcurr = 'TRY'
	AND CONVERT(DATE, gdatu, 104) = CAST(GETDATE() AS DATE)) c on a.rtcur = c.fcurr
),

main_cte AS (
SELECT 
	rbukrs,
	acdoca.kunnr,
	kna1.name1 AS client_company,
	EOMONTH(CAST(budat AS DATE)) AS [date],
	amount = SUM(CASE WHEN blart <> '' AND racct IN ('1280102000', '1200102000', '1010001020', '1210001020')THEN hsl ELSE NULL END),
	cheque_bill = SUM(CASE WHEN racct IN ('1010001010', '1210001010') THEN hsl ELSE NULL END),
	bank_guarantee = SUM(CASE
                             WHEN blart <> '' AND umskz = '1' THEN amount 
                             ELSE NULL 
						END),
	cash_collateral = SUM(CASE
                             WHEN blart <> '' AND umskz = 'Q' THEN amount 
                             ELSE NULL 
						END),
	expense = SUM(CASE WHEN blart = 'DM' AND racct LIKE '120%' THEN hsl ELSE NULL END)
FROM acdoca
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} kna1 ON acdoca.kunnr = kna1.kunnr 
GROUP BY
	rbukrs,
	acdoca.kunnr,
	kna1.name1,
	EOMONTH(CAST(budat AS DATE))
),

summed_cte AS (
SELECT 
	rbukrs AS [company],
	kunnr AS [customer],
	client_company,
	[date],
	SUM(amount) OVER(PARTITION BY rbukrs, kunnr ORDER BY [date]) AS balance,
	SUM(cheque_bill) OVER(PARTITION BY rbukrs, kunnr ORDER BY [date]) AS cheque_bill,
	SUM(bank_guarantee) OVER(PARTITION BY rbukrs, kunnr ORDER BY [date]) AS bank_guarantee,
	SUM(cash_collateral) OVER(PARTITION BY rbukrs, kunnr ORDER BY [date]) * (-1) AS cash_collateral,
	expense,
	in_expense = CASE WHEN expense IS NOT NULL THEN rbukrs ELSE NULL END,
	out_expense = CASE WHEN expense IS NULL THEN rbukrs ELSE NULL END
FROM main_cte
),

final_cte AS (
SELECT 
	[company],
	customer,
	client_company,
	[date],
	total_balance = CASE WHEN cheque_bill IS NOT NULL THEN balance + cheque_bill ELSE balance END,
	balance,
	cheque_bill,
	bank_guarantee,
	cash_collateral,
	CASE 
		WHEN (CASE WHEN cheque_bill IS NOT NULL THEN balance + cheque_bill ELSE balance END) IS NULL THEN (
				CASE 
					WHEN bank_guarantee IS NOT NULL AND cash_collateral IS NOT NULL THEN (bank_guarantee * (-1) - cash_collateral)
					WHEN bank_guarantee IS NULL AND cash_collateral IS NOT NULL THEN (cash_collateral * (-1))
					WHEN bank_guarantee IS NOT NULL AND cash_collateral IS NULL THEN (bank_guarantee * (-1))
				END)
		ELSE(
			CASE
				WHEN bank_guarantee IS NOT NULL AND cash_collateral IS NULL THEN (CASE WHEN cheque_bill IS NOT NULL THEN balance + cheque_bill ELSE balance END) - bank_guarantee
				WHEN bank_guarantee IS NULL AND cash_collateral IS NOT NULL THEN (CASE WHEN cheque_bill IS NOT NULL THEN balance + cheque_bill ELSE balance END) - cash_collateral
				WHEN bank_guarantee IS NOT NULL AND cash_collateral IS NOT NULL THEN (CASE WHEN cheque_bill IS NOT NULL THEN balance + cheque_bill ELSE balance END) - (bank_guarantee + cash_collateral)
				WHEN bank_guarantee IS NULL AND cash_collateral IS NULL THEN (CASE WHEN cheque_bill IS NOT NULL THEN balance + cheque_bill ELSE balance END)
			END)
	END AS risk,
	expense,
	in_expense,
	out_expense
	--ROW_NUMBER() OVER(PARTITION BY [date] ORDER BY (CASE WHEN cheque_bill IS NOT NULL THEN balance + cheque_bill ELSE balance END) DESC) AS rn_sap,
	--ROW_NUMBER() OVER(PARTITION BY ad.yil, ad.ay ORDER BY ad.[ay_sonu_toplam_bakiye] DESC) AS rn_ad
FROM summed_cte sc
--FULL JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_customerriskadjustments') }} ad --ON sc.[company] = ad.sirket_kodu
																                        --ON sc.customer = CAST(ad.musteri_kodu AS NVARCHAR)
																                        --AND YEAR(sc.[date]) = ad.yil
																                        --AND MONTH(sc.[date]) = ad.ay
),

date_series AS (
    SELECT CAST('2023-12-31' AS DATE) AS [date]
    UNION ALL
    SELECT EOMONTH(DATEADD(MONTH, 1, [date]))
    FROM date_series
    WHERE [date] < '2026-12-31'
),

entity_list AS (
    SELECT DISTINCT company, customer FROM final_cte
),

all_dates AS (
    SELECT ds.[date], e.company, e.customer
    FROM date_series ds
    CROSS JOIN entity_list e
),

total_other_expense AS (
    SELECT 
        rbukrs AS company,
        kunnr AS customer,
        SUM(CASE WHEN blart = 'DM' AND racct LIKE '%1200102000%' THEN hsl ELSE 0 END) AS other_expense
    FROM {{ ref('stg__s4hana_t_sap_acdoca') }}
    GROUP BY rbukrs, kunnr
),

filtered_expense AS (
   SELECT
        rbukrs AS company,
        kunnr AS customer,
        MONTH(CAST(budat AS DATE)) AS [month],
		YEAR(CAST(budat AS DATE)) AS [year],
        SUM(CASE WHEN blart = 'DM' AND racct LIKE '120%' THEN hsl ELSE 0 END) AS expense
    FROM {{ ref('stg__s4hana_t_sap_acdoca') }}
	where 1=1
    GROUP BY rbukrs, kunnr, YEAR(CAST(budat AS DATE)), MONTH(CAST(budat AS DATE))
),

joined AS (
    SELECT 
        a.[date],
        a.company,
        a.customer,
		f.client_company,
        f.[date] AS balance_date,
        f.total_balance,
		f.balance,
		f.cheque_bill,
		f.bank_guarantee,
		f.cash_collateral,
		f.risk,
		fe.expense,
		toe.other_expense,
		f.in_expense,
		f.out_expense
    FROM all_dates a
    LEFT JOIN final_cte f 
        ON a.company = f.company 
        AND a.customer = f.customer 
        AND f.[date] <= a.[date]
	LEFT JOIN total_other_expense toe 
		ON a.company = toe.company 
		AND a.customer = toe.customer
    LEFT JOIN filtered_expense fe
        ON a.company = fe.company
        AND a.customer = fe.customer
        AND (YEAR(a.[date]) = fe.[year] AND MONTH(a.[date]) = fe.[month])
),

ranked AS (
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY company, customer, [date] ORDER BY balance_date DESC) AS rn
    FROM joined
)

SELECT 
	rls_region,
	rls_group,
	rls_company = CONCAT('_', rls_region),
	rls_businessarea = CONCAT('_', rls_region),
	--[company],
	customer,
	ad.musteri_kodu AS customer_adjusted,
	client_company,
	[date],
	SUM(total_balance) AS total_balance,
	MAX(ad.[ay_sonu_toplam_bakiye]) AS total_balance_adjusted,
	SUM(balance) AS balance,
	MAX(ad.[bakiye]) AS balance_adjusted,
	MAX(ad.[0_gun_dusulmus_bakiye]) AS zero_days_deducted_balance,
	SUM(cheque_bill) AS cheque_bill,
	MAX(ad.[cek_senet]) AS cheque_bill_adjusted,
	SUM(bank_guarantee) AS bank_guarantee,
	MAX(ad.[teminat]) AS bank_guarantee_adjusted,
	SUM(cash_collateral) AS cash_collateral,
	SUM(ranked.risk) AS risk,
	MAX(ad.[risk]) AS risk_adjusted,
	MAX(ad.[0_gun_dusulmus_risk]) AS zero_days_deducted_risk,
	SUM(expense) AS expense,
	SUM(other_expense) AS other_expense,
	ad.[risk_duzeyi] AS risk_level_adjusted,
	--CASE
	--	WHEN expense <> 0 THEN (
	--		CASE
	--			WHEN (ranked.risk / expense) >= 3 OR (risk / expense) = 0 AND out_expense IS NOT NULL THEN 'High'
	--			WHEN (ranked.risk / expense) BETWEEN 1.5 AND 3 THEN 'Medium'
	--			WHEN (ranked.risk / expense) <= 1.5 THEN 'Low'
	--			ELSE NULL 
	--		END )
	--	ELSE NULL
	--END AS risk_level,
	--CASE 
	--	WHEN customer IS NOT NULL AND ad.musteri_kodu IS NOT NULL THEN (
	--		CASE
	--			WHEN rn_sap <= 50 AND rn_ad <= 50 THEN '1'
	--			WHEN rn_sap > 50 AND rn_ad <= 50 THEN '0'
	--			ELSE NULL
	--		END )
	--	ELSE NULL
	--END AS top_fifty_state,
	ad.[cikarilma_nedeni] AS removal_reason
FROM ranked
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON ranked.[company] = dim_comp.RobiKisaKod
FULL JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_customerriskadjustments') }} ad ON customer = CAST(ad.musteri_kodu AS NVARCHAR)
																                        AND YEAR([date]) = ad.yil
																                        AND MONTH([date]) = ad.ay
WHERE 1=1
AND dim_comp.KyribaGrup = N'RGYGROUP' 
AND [company] IN ('ESE', 'FER', 'KUR', 'GOK', 'KOZ', 'MEL', 'ML3', 'ML4', 'TAR', 'SAL', 'BAK', 'ALT')
AND rn = 1
GROUP BY
	rls_region,
	rls_group,
	CONCAT('_', rls_region),
	customer,
	ad.musteri_kodu,
	client_company,
	[date],
	ad.[risk_duzeyi],
	ad.[cikarilma_nedeni]