{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

WITH currencies AS (
SELECT 
	CAST(date_value AS DATE) [date],
    try_value,
	usd_value,
	eur_value
FROM {{ ref('stg__dimensions_t_dim_dailys4currencies') }}
WHERE currency = 'TRY'
),

main_cte AS (
SELECT
	racct AS account_number,
	rbukrs AS [company],
	rbusa AS business_area,
	t001w.name1 AS business_area_description,
	--p.[name] AS portfolio_name,
	gjahr AS fiscal_year,
	CAST(budat AS DATE) AS posting_date,
	amount_try = SUM(hsl),
	amount_eur = SUM(hsl) * currencies.eur_value,
	amount_usd = SUM(hsl) * currencies.usd_value,
	amount_type = 
		CASE
			WHEN LEFT(racct, 3)  = '101' THEN 'Cheque'
			WHEN LEFT(racct, 3) = '121' THEN 'Bill' 
			ELSE NULL 
		END
FROM {{ ref('stg__s4hana_t_sap_acdoca') }} acdoca
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON acdoca.rbusa = t001w.bwkey
LEFT JOIN currencies ON CAST(acdoca.budat AS DATE) = currencies.[date]
--LEFT JOIN rgy_kpi.raw__rgy_kpi_t_dim_portfolio p ON acdoca.rbusa = p.WERKS
WHERE (LEFT(racct, 3) = '101' OR LEFT(racct, 3) = '121')
GROUP BY 
	racct,
	rbukrs,
	rbusa,
	t001w.name1,
	currencies.eur_value,
	currencies.usd_value,
	--p.[name],
	--p.[WERKS],
	gjahr,
	budat
)
SELECT 
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(main_cte.business_area, '_', rls_region),
	main_cte.*
FROM main_cte
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON main_cte.[company] = dim_comp.robikisakod
WHERE dim_comp.KyribaGrup = N'RGYGROUP'