{{
  config(
    materialized = 'table',tags = ['gyg_kpi']
    )
}}

WITH gyg_realized_year_month AS (
    SELECT *
    FROM {{ ref('stg__gyg_kpi_t_fact_realizedgygdetailed') }}
)
SELECT 
    rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,g.company
	,document_number
	,line_item
	,account_number
	,year_int
	,month_int
	,financial_center_code_adjusted as financial_center_code
	,commitment_item_code_adjusted as commitment_item_code
	,amount_try
	,amount_usd
	,amount_eur
FROM gyg_realized_year_month g
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON g.company = dim_comp.RobiKisaKod
WHERE dim_comp.kyriba_ust_group = N'RÖNESANS'
