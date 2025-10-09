{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
WITH raw_data AS (
SELECT
	TRIM(CAST([financial_center_code] AS nvarchar)) AS [financial_center_code]
	,UPPER(fmcit.TEXT1) AS financial_center_code_description
	,TRIM(CAST(br.commitment_item_code AS nvarchar)) AS [commitment_item_code]
	,fmcit2.TEXT1 as commitment_item_description
	,[year_month]
	,EOMONTH(CAST([year_month] + '-01' AS DATE)) date
	,COALESCE(CAST([budget_try] AS money), 0) AS [budget_try] 
	,COALESCE(CAST([budget_usd] AS money), 0) AS  [budget_usd]
	,COALESCE(CAST([budget_eur] AS money), 0) AS [budget_eur]  
	,COALESCE(CAST([realized_try] AS money), 0) AS [realized_try] 
	,COALESCE(CAST([realized_usd] AS money), 0) AS [realized_usd] 
	,COALESCE(CAST([realized_eur] AS money), 0) AS  [realized_eur]
  FROM {{ source('stg_sharepoint', 'raw__rmh_kpi_t_fact_budgetandrealized') }} br
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} AS fmcit ON CAST([financial_center_code] AS nvarchar) = fmcit.FIPEX
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} AS fmcit2 ON TRIM(CAST(br.commitment_item_code AS nvarchar)) = fmcit2.FIPEX
	)

SELECT
	rd.financial_center_code
	,rd.financial_center_code_description
	,rd.commitment_item_code
	,rd.commitment_item_description
	,rd.year_month
	,budget_try
	,COALESCE(budget_try * curr.usd_value, 0) AS budget_usd
	,COALESCE(budget_try * curr.eur_value, 0) AS budget_eur
	,realized_try
	,COALESCE(realized_try * curr.usd_value, 0) AS realized_usd
	,COALESCE(realized_try * curr.eur_value, 0) AS realized_eur
FROM raw_data rd
  LEFT JOIN {{ ref('dm__dimensions_t_dim_dailys4currencies') }} curr ON rd.date = curr.date_value
																			AND currency = 'TRY'