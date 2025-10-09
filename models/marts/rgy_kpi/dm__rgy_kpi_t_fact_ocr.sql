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
	portfolioname AS portfolio_name,
	brand,
	RGYMainSector AS main_sector,
	RGYSectorLVL1 AS sub_sector,
	[year],
	t.[name] AS [month],
	rent,
	GiroRent AS giro_rent,
	RentDiscount AS rent_discount,
	CommonAreaRent AS common_area_rent,
	TotalGiro AS total_giro,
	calculated_ocr = (GiroRent + CommonAreaRent + Rent) / TotalGiro
FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_ocrreport') }} ocr
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_term') }} t ON ocr.TermID = t.ID
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} portfolio ON PortfolioID = portfolio.ID
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} t001k ON portfolio.werks = t001k.bwkey
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON t001k.bukrs = dim_comp.RobiKisaKod
WHERE 1=1
	AND TotalGiro <> 0 
	AND (Rent + GiroRent) <> 0
	AND SiteTypeName = N'Dükkan'