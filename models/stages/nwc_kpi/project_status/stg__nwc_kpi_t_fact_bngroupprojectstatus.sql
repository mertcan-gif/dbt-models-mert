
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','projectstatus']
    )
}}


SELECT 
  company = company	collate database_default
  ,projects = projects collate database_default
  ,business_area_code = CONCAT('BNP_',CAST(DENSE_RANK() OVER(ORDER BY projects) AS NVARCHAR))
  ,contract_currency = contract_currency	collate database_default
  ,budget_period = CAST('' AS NVARCHAR)
  ,budget_revenue	
  ,budget_cost	
  ,budget_profit  = budget_revenue-budget_cost
  ,revenue_realization	= poc*budget_cost
  ,cash_flow	
  ,withholding_tax =	0
  ,fixed_budget =	0
FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_bnprojectstatus') }}


