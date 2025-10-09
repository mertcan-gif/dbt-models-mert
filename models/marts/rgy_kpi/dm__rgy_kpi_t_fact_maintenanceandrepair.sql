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
	m.id,
	portfoy AS portfolio_name,
	t001w.name1 AS business_area_description,
	ekipman AS equipment,
	periyot AS [period],
	bakimi_yapacak_firma AS maintenance_type,
	CAST(baslangic_tarihi AS DATE) AS [start_date],
	CAST(bitis_tarihi AS DATE) AS end_date
FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_maintenanceandrepair') }} m
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} p ON m.portfoy_id = p.ID
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON p.WERKS = t001w.bwkey
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} t001k ON p.WERKS = t001k.bwkey
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON t001k.bukrs = dim_comp.RobiKisaKod