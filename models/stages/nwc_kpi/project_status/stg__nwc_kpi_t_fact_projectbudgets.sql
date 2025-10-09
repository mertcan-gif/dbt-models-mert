
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','projectstatus']
    )
}}

WITH Currencies AS (
	SELECT
		[month] = MONTH(date_value)
		,year_month = CONCAT(YEAR(date_value),'-',MONTH(date_value))
		,date_value
		,currency
		,try_value
		,eur_value
		,usd_value
	FROM {{ ref('stg__dimensions_t_dim_dailys4currencies') }}
	WHERE 1=1
		AND DATEPART(d,DATEADD(d,1,date_value)) = 1 -- Last day of a month
		AND YEAR(date_value) >= DATEADD(YEAR,-1,YEAR(GETDATE()))
		
)

SELECT 
	company = 'REC'
	,projects = t.NAME1
	,business_area_code = GSBER
	,contract_currency = pb
	,budget_revenue = revenue
	,budget_revenue_eur = (CASE WHEN pb = 'EUR' THEN revenue
							   WHEN pb = 'TRY' THEN revenue*(SELECT eur_value FROM Currencies WHERE Currencies.currency = rb.pb AND Currencies.date_value = EOMONTH(CAST(CONCAT(budget_year,'-',budget_month,'-01') AS DATE)))
							   WHEN pb = 'USD' THEN (revenue*(1/(SELECT usd_value FROM Currencies WHERE Currencies.currency = rb.pb AND Currencies.date_value = EOMONTH(CAST(CONCAT(budget_year,'-',budget_month,'-01') AS DATE))))*(SELECT eur_value FROM Currencies WHERE Currencies.currency = rb.pb AND Currencies.date_value = EOMONTH(CAST(CONCAT(budget_year,'-',budget_month,'-01') AS DATE))))
						END)	
	,budget_cost = cost
	,budget_profit = revenue-cost
	,revenue_realization = NULL
	,cash_flow = NULL
	,withholding_tax = NULL
	,fixed_budget
	,reporting_date = EOMONTH(DATEFROMPARTS(budget_year,budget_month,'01'))
	
FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }} rb
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t ON t.WERKS collate database_default = rb.GSBER
WHERE revenue <> 0


UNION ALL 

SELECT 
	company = company	collate database_default
	,projects = projects collate database_default
	,business_area_code = '1' collate database_default
	,contract_currency = contract_currency	collate database_default
	,budget_revenue	
	,budget_revenue_eur = budget_revenue
	,budget_cost	
	,budget_profit  = budget_revenue-budget_cost
	,revenue_realization	= poc*budget_cost
	,cash_flow	
	,withholding_tax =	0
	,fixed_budget =	0
	,CAST(reporting_date AS DATE)
FROM {{ source('stg_sharepoint','raw__nwc_kpi_t_fact_bnprojectstatus' ) }}
