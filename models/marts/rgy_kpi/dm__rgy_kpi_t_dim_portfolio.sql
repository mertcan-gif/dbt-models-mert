{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(t001w.bwkey, '_', rls_region),
	t001k.bukrs AS company_code,
	t001w.bwkey AS business_area_code,
	t001w.name1 AS business_area_description,
	portfolio.[name] AS portfolio_name_rem
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} t001k ON t001w.bwkey = t001k.bwkey
RIGHT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} portfolio ON t001w.bwkey = portfolio.WERKS
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON t001k.bukrs = dim_comp.RobiKisaKod
WHERE t001w.bwkey IS NOT NULL