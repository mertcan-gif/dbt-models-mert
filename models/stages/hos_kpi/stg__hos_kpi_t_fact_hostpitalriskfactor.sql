{{
  config(
    materialized = 'table',tags = ['hos_kpi']
    )
}}

SELECT 
	TRIM([business_area]) [business_area]
	,CAST([high_risk_cost_eur] AS money) [high_risk_cost_eur]
	,CAST([medium_risk_cost_eur] AS money) [medium_risk_cost_eur]
	,CAST([low_risk_cost_eur] AS money) [low_risk_cost_eur]
	,TRIM(cast([year] as nvarchar)) as [year]
FROM  {{ source('stg_sharepoint', 'raw__hos_kpi_t_fact_hospitalriskfactorcosts') }}