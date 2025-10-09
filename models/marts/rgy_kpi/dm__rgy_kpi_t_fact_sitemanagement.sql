{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(portfolio.werks, '_', rls_region),
	PortfolioName AS portfolio_name,
	s.Squaremeter AS m2,
	MADate AS opening_date,
	PhysicalSiteState AS physical_site_state,
	SiteStatus AS site_state
FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_sitemanagementlist') }} s
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} portfolio ON s.PortfolioID = portfolio.ID
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} t001k ON portfolio.werks = t001k.bwkey
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON t001k.bukrs = dim_comp.RobiKisaKod

WHERE 1=1
	AND IsVirtual = '0'
	AND SiteTypeName = 'Dükkan'