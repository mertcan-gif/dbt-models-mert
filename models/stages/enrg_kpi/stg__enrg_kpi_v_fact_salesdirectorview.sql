{{
  config(
    materialized = 'view',tags = ['enrg_kpi']
    )
}}	
SELECT DISTINCT
    COALESCE(plants.region,'NAR') as rls_region 
	,CONCAT(plants.[group],'_',plants.[region]) as rls_group
	,CONCAT(plants.company,'_',plants.[region]) as rls_company
	,CONCAT(acdoca.RBUSA,'_',COALESCE(plants.region,'NAR')) as rls_businessarea
	,acdoca.RBUKRS as company
	,acdoca.GJAHR as fiscal_year
	,MONTH(acdoca.BUDAT) as month
	,acdoca.RBUSA as 'business_area'
	,SUM(CAST(acdoca.HSL AS MONEY))*(-1) as 'realized_profit_tl'
	,SUM(CAST(acdoca.KSL AS MONEY))*(-1) as 'realized_profit_eur'
	,SUM(CAST(acdoca.OSL AS MONEY))*(-1) as 'realized_profit_usd'
FROM  {{ ref('stg__s4hana_t_sap_acdoca') }} as acdoca
	LEFT JOIN {{ source('stg_dimensions', 'raw__enrg_kpi_t_dim_powerplants') }} as plants ON acdoca.RBUKRS collate database_default = plants.company
WHERE 1=1
		AND acdoca.RBUKRS collate database_default IN (SELECT company FROM {{ source('stg_dimensions', 'raw__enrg_kpi_t_dim_powerplants') }}) 
		AND ([RACCT] LIKE '6%' OR [RACCT] LIKE '7%')
		-- Son 2 ay'ın KAR'ı alınmayacak dendiği için alttaki satır yazıldı.
		AND (acdoca.GJAHR < YEAR(GETDATE()) OR (acdoca.GJAHR = YEAR(GETDATE()) AND MONTH(acdoca.BUDAT) <= MONTH(GETDATE()) - 2))
GROUP BY  plants.region
		, CONCAT(plants.[group],'_',plants.[region])
		, CONCAT(plants.company,'_',plants.[region])
		, CONCAT(acdoca.RBUSA,'_',COALESCE(plants.region,'NAR'))
		, RBUKRS
		, RBUSA
		, GJAHR
		, MONTH(BUDAT)