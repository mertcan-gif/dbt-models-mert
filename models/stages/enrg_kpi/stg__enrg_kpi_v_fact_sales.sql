
{{
  config(
    materialized = 'view',tags = ['enrg_kpi']
    )
}}	
SELECT DISTINCT
    COALESCE(plants.region,'NAR') as rls_region 
	,CONCAT(plants.[group],'_',plants.[region]) as rls_group
	,CONCAT(plants.company,'_',plants.[region]) as rls_company
	,rls_businessarea=concat('_',COALESCE(plants.region,'NAR'))
	,acdoca.RBUKRS as company
	,acdoca.GJAHR as fiscal_year
	,MONTH(acdoca.BUDAT) as month
	,SUM(CAST(acdoca.HSL AS FLOAT))*(-1) as 'realized_profit_tl'
	,SUM(CAST(acdoca.KSL AS FLOAT))*(-1) as 'realized_profit_eur'  -- EUR
	,SUM(CAST(acdoca.OSL AS FLOAT))*(-1) as 'realized_profit_usd'  -- EUR
	,SUM (CASE WHEN acdoca.RACCT IN ('6000106001','6000206001','6000206016','6000106020') THEN CAST(acdoca.HSL AS FLOAT) ELSE 0 END) as 'sales_amount_tl'  --in+out+otc+in_wholesale
	,SUM (CASE WHEN acdoca.RACCT = '6000106001' THEN CAST(acdoca.HSL AS FLOAT) ELSE 0 END) as 'in_tl'
	,SUM (CASE WHEN acdoca.RACCT = '6000206001' THEN CAST(acdoca.HSL AS FLOAT) ELSE 0 END) as 'out_tl'
	,SUM (CASE WHEN acdoca.RACCT = '6000206016' THEN CAST(acdoca.HSL AS FLOAT) ELSE 0 END) as 'otc_tl'
	,SUM (CASE WHEN acdoca.RACCT = '6000106020' THEN CAST(acdoca.HSL AS FLOAT) ELSE 0 END) as 'in_wholesale_tl'
	,SUM (CASE WHEN acdoca.RACCT IN ('6000106001','6000206001','6000206016','6000106020') THEN CAST(acdoca.KSL AS FLOAT) ELSE 0 END) as 'sales_amount_eur'  --in+out+otc+in_wholesale -- EUR
	,SUM (CASE WHEN acdoca.RACCT = '6000106001' THEN CAST(acdoca.KSL AS FLOAT) ELSE 0 END) as 'in_eur' -- EUR
	,SUM (CASE WHEN acdoca.RACCT = '6000206001' THEN CAST(acdoca.KSL AS FLOAT) ELSE 0 END) as 'out_eur' -- EUR
	,SUM (CASE WHEN acdoca.RACCT = '6000206016' THEN CAST(acdoca.KSL AS FLOAT) ELSE 0 END) as 'otc_eur' -- EUR
	,SUM (CASE WHEN acdoca.RACCT = '6000106020' THEN CAST(acdoca.KSL AS FLOAT) ELSE 0 END) as 'in_wholesale_eur' -- EUR
	,SUM (CASE WHEN acdoca.RACCT IN ('6000106001','6000206001','6000206016','6000106020') THEN CAST(acdoca.OSL AS FLOAT) ELSE 0 END) as 'sales_amount_usd'  --in+out+otc+in_wholesale -- usd
	,SUM (CASE WHEN acdoca.RACCT = '6000106001' THEN CAST(acdoca.OSL AS FLOAT) ELSE 0 END) as 'in_usd' -- usd
	,SUM (CASE WHEN acdoca.RACCT = '6000206001' THEN CAST(acdoca.OSL AS FLOAT) ELSE 0 END) as 'out_usd' -- usd
	,SUM (CASE WHEN acdoca.RACCT = '6000206016' THEN CAST(acdoca.OSL AS FLOAT) ELSE 0 END) as 'otc_usd' -- usd
	,SUM (CASE WHEN acdoca.RACCT = '6000106020' THEN CAST(acdoca.OSL AS FLOAT) ELSE 0 END) as 'in_wholesale_usd' -- usd
	,SUM(CASE WHEN acdoca.RACCT = '6000106001' THEN CAST(acdoca.MSL AS FLOAT)/1000000 ELSE 0 END)*(-1) as  'in_gwh'
	,SUM(CASE WHEN acdoca.RACCT = '6000206001' THEN CAST(acdoca.MSL AS FLOAT)/1000000 ELSE 0 END)*(-1) as 'out_gwh'
	,SUM(CASE WHEN acdoca.RACCT = '6000106020' THEN CAST(acdoca.MSL AS FLOAT)/1000000 ELSE 0 END)*(-1) as 'in_wholesale_gwh'
	,SUM(CASE WHEN acdoca.RACCT = '6000206016' THEN CAST(acdoca.MSL AS FLOAT)/1000000 ELSE 0 END)*(-1) as 'otc_wholesale_gwh'
	,SUM(CASE WHEN acdoca.RACCT IN ('6000106001','6000206001','6000106020','6000206016') THEN CAST(acdoca.MSL AS FLOAT)/1000000 ELSE 0 END)*(-1) 'sales_amount_gwh'
	,bgwh.otc_budget_gwh as 'otc_budget_gwh'
	,bgwh.st_budget_gwh as 'st_budget_gwh'
	,bgwh.budget_gwh as 'total_budget_gwh'
FROM {{ ref('stg__s4hana_t_sap_acdoca') }} as acdoca
	LEFT JOIN {{ source('stg_dimensions', 'raw__enrg_kpi_t_dim_powerplants') }} as plants ON acdoca.RBUKRS = plants.company collate database_default
	LEFT JOIN {{ source('stg_enrg_kpi', 'raw__enrg_kpi_t_fact_budgetsgwh') }} as bgwh ON acdoca.RBUKRS = bgwh.company  collate database_default
																	AND acdoca.GJAHR = bgwh.[year]
																	AND MONTH(acdoca.BUDAT) = bgwh.[month]
	WHERE 1=1
			AND acdoca.RBUKRS collate database_default IN (SELECT company FROM {{ source('stg_dimensions', 'raw__enrg_kpi_t_dim_powerplants') }}) 
			AND ([RACCT] LIKE '6%' OR [RACCT] LIKE '7%')
			-- Son 2 ay'ın KAR'ı alınmayacak dendiği için alttaki satır yazıldı.
			AND (acdoca.GJAHR < YEAR(GETDATE()) OR (acdoca.GJAHR = YEAR(GETDATE()) AND MONTH(acdoca.BUDAT) <= MONTH(GETDATE()) - 2))
	GROUP BY  plants.region
			, CONCAT(plants.[group],'_',plants.[region])
			, CONCAT(plants.company,'_',plants.[region])
			, RBUKRS
			, GJAHR
			, MONTH(BUDAT)
			, bgwh.otc_budget_gwh
			, bgwh.st_budget_gwh
			, bgwh.budget_gwh