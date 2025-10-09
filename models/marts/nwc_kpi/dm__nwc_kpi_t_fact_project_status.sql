
{{
  config(
    materialized = 'table',tags = ['nwc_kpi_draft','projectstatus_draft']
    )
}}

WITH Currencies AS (
	SELECT  
		[date_value]
		,[date_value_string]
		,[date_string]
		,[currency]
		,[try_value]
		,[usd_value]
		,[eur_value]
	FROM {{ ref('stg__dimensions_t_dim_dailys4currencies') }}
	WHERE 1=1 
		AND date_value = (SELECT MAX(DATE_VALUE) from {{ ref('stg__dimensions_t_dim_dailys4currencies') }})
		AND currency = 'try'
		
)

SELECT 
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(company,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] =  CONCAT(COALESCE(ALL_CTE.business_area_code ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,ALL_CTE.company
	,ALL_CTE.projects
	,ALL_CTE.business_area_code
	,ALL_CTE.budget_period
	,ALL_CTE.contract_currency
	,currency_value = (CASE WHEN ALL_CTE.contract_currency = 'TRY' THEN (SELECT try_value FROM Currencies)
							 WHEN ALL_CTE.contract_currency = 'USD' THEN (SELECT usd_value FROM Currencies)
							 WHEN ALL_CTE.contract_currency = 'EUR' THEN (SELECT eur_value FROM Currencies)
						END)
	,budget_revenue_try = (CASE WHEN ALL_CTE.contract_currency = 'TRY' THEN ALL_CTE.budget_revenue
							   WHEN ALL_CTE.contract_currency = 'USD' THEN ALL_CTE.budget_revenue*(1/(SELECT usd_value FROM Currencies))
							   WHEN ALL_CTE.contract_currency = 'EUR' THEN ALL_CTE.budget_revenue*(1/(SELECT eur_value FROM Currencies))
						END)
	,budget_revenue_usd = (CASE WHEN ALL_CTE.contract_currency = 'USD' THEN ALL_CTE.budget_revenue
							   WHEN ALL_CTE.contract_currency = 'TRY' THEN ALL_CTE.budget_revenue*(SELECT usd_value FROM Currencies)
							   WHEN ALL_CTE.contract_currency = 'EUR' THEN (ALL_CTE.budget_revenue*(1/(SELECT eur_value FROM Currencies))*(SELECT usd_value FROM Currencies))
						END)
	,budget_revenue_eur = (CASE WHEN ALL_CTE.contract_currency = 'EUR' THEN ALL_CTE.budget_revenue
							   WHEN ALL_CTE.contract_currency = 'TRY' THEN ALL_CTE.budget_revenue*(SELECT eur_value FROM Currencies)
							   WHEN ALL_CTE.contract_currency = 'USD' THEN (ALL_CTE.budget_revenue*(1/(SELECT usd_value FROM Currencies))*(SELECT eur_value FROM Currencies))
						END)	
	,budget_revenue = CASE 
						  WHEN ALL_CTE.fixed_budget = '1' THEN ALL_CTE.revenue_realization
						  ELSE ALL_CTE.budget_revenue
					  END
	,budget_cost = CASE 
						WHEN ALL_CTE.fixed_budget = '1' THEN ALL_CTE.revenue_realization
						ELSE ALL_CTE.budget_cost
					END
	,ALL_CTE.budget_profit
	,ALL_CTE.revenue_realization
	,ALL_CTE.cash_flow
	,ALL_CTE.withholding_tax
	,kuc.KyribaGrup AS kyriba_group
	,kuc.KyribaKisaKod AS kyriba_company_code


FROM (
      SELECT * FROM {{ ref('stg__nwc_kpi_t_fact_projectstatusrti') }}
      UNION ALL 
      SELECT * FROM {{ ref('stg__nwc_kpi_t_fact_bngroupprojectstatus') }} 
	  ) ALL_CTE
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON ALL_CTE.company = kuc.RobiKisaKod collate database_default
