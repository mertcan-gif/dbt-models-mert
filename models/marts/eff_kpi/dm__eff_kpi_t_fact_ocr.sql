{{
  config(
    materialized = 'table',tags = ['eff_kpi']
    )
}}

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(portfolio.werks, '_', rls_region),
	portfolio_name = PortfolioName,
	brand,
	[year],
	common_area_income = CommonAreaRent,
	total_rental_income = 
		CASE
			WHEN RentDiscount >= 0 THEN GiroRent + Rent - RentDiscount
			WHEN RentDiscount < 0 THEN GiroRent + Rent + RentDiscount
			ELSE 0
		END,
	total_turnover = TotalGiro,
	discount_rent = RentDiscount,
	base_ocr = ContractOCR,
	discounted_ocr = OCR
FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_ocrreport') }} ocr
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} portfolio ON ocr.portfolioid = portfolio.ID
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} t001k ON portfolio.WERKS = t001k.bwkey
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON t001k.bukrs = dim_comp.RobiKisaKod
WHERE SiteTypeName = N'Dükkan'
AND [year] > 2018