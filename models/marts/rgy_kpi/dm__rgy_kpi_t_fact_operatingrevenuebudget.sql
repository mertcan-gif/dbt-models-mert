{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(p.WERKS, '_', rls_region),
	budget.*
FROM {{ ref('stg__rgy_kpi_t_fact_operatingrevenuebudget') }} budget
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} p ON budget.portfolio_id = p.ID
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }}  dim_comp ON p.BusinessArea = dim_comp.RobiKisaKod
