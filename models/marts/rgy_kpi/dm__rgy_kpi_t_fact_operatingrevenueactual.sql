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
    actual.*
FROM {{ ref('stg__rgy_kpi_t_fact_operatingrevenueactual') }} actual
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} p ON actual.portfolio_id = p. id
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON actual.[company] = dim_comp.RobiKisaKod