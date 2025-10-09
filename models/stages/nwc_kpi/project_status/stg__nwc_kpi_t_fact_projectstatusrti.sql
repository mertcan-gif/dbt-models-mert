
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','projectstatus']
    )
}}

SELECT 
	company = bv.bukrs
	,projects = CASE 
					WHEN business_area_description IS NOT NULL THEN business_area_description 
					ELSE bv.gtext 
				END
	,business_area_code = bv.GSBER
	,contract_currency = pb
	,budget_period = [Budget Period]
	,budget_revenue = revenue
	,budget_cost = cost
	,budget_profit = revenue-cost
	,revenue_realization = CASE 
							   WHEN pb = 'TRY' THEN revenue_realization_try 
							   WHEN pb = 'EUR' THEN revenue_realization_eur 
						   ELSE revenue_realization_usd END
	,cash_flow = CASE 
					 WHEN pb = 'TRY' THEN cash_flow_try 
					 WHEN pb = 'EUR' THEN cash_flow_eur 
				 ELSE cash_flow_usd END
	,withholding_tax = CASE 
						   WHEN pb = 'TRY' THEN withholding_tax_try 
						   WHEN pb = 'EUR' THEN withholding_tax_eur 
					   ELSE withholding_tax_usd END
	,fixed_budget

FROM (
      SELECT * 
      FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }}
      WHERE 1=1
	  	AND revenue <> 0
        AND Budget_Year = (SELECT MAX(budget_year) FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }})
        AND Budget_Month = (SELECT MAX(budget_month) FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }} WHERE budget_year = YEAR(GETDATE()))
    ) bv

	LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_projectstatusaccrualdetails') }} th ON th.company = bv.bukrs 
																			 AND th.business_area = bv.GSBER
	LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_projectstatuscashflowdetails') }} cf ON cf.RBUSA = bv.GSBER
																			  AND cf.company = bv.bukrs
	LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_projectstatuswithholdingtax') }} stopaj ON stopaj.RBUSA = bv.GSBER
