{{
  config(
    materialized = 'table',tags = ['eff_kpi']
    )
}}

WITH cte_main AS (
	SELECT 
		ocr.portfolioid as portfolio_id,
		ocr.portfolioname as portfolio_name,
		ocr.brand as brand,
		ocr.sitename as site_name,
		ocr.sitetypename as site_type,
		camtype,
		ocr.[year],
		COUNT(distinct (tr.[name])) as count_month,
		ocr.sitegla as gla,
		SUM(ocr.commonarearent) as common_area_rent,
		SUM(ocr.rent) as rent,
		SUM(ocr.girorent) as giro_rent,
		SUM(ocr.RentDiscount) as discounted_rent
	FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_ocrreport') }} ocr
	LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_contractmanagementlist') }} cm ON ocr.contractcode = cm.contractcode
																		AND ocr.sitename = cm.sitename
	LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_term') }} tr ON ocr.termid = tr.id
	WHERE ocr.SiteTypeName = N'Dükkan'
	GROUP BY 
		ocr.portfolioid,
		ocr.portfolioname,
		ocr.sitename,
		ocr.sitetypename,
		ocr.brand,
		camtype,
		ocr.sitegla,
		ocr.[year]
),

giro_per_mall AS (
	SELECT 
	Portfolioname,
	[year],
	SUM(commonarearent) as rent_m2_mall,
	SUM(sitegla) as m2_per_mall,
	gla_income_mall =
		CASE
			WHEN SUM(sitegla) <> 0 THEN SUM(commonarearent) / SUM(sitegla)
			ELSE 0
		END
	FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_ocrreport') }}
	GROUP BY 
	Portfolioname,
	[year]
),

giro_per_brand AS (
	SELECT 
	brand,
	[year],
	SUM(commonarearent) as rent_m2_brand,
	SUM(sitegla) as m2_per_brand,
	gla_income_brand = 
			CASE 
				WHEN SUM(sitegla) <> 0 THEN SUM(commonarearent) / SUM(sitegla)
				ELSE 0
			END
	FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_ocrreport') }}
	GROUP BY 
	Brand,
	[year]
),

final AS (
	SELECT
		rls_region,
		rls_group,
		rls_company,
		rls_businessarea = CONCAT(portfolio.werks, '_', rls_region),
		cte.*,
		gla_income_mall,
		gla_income_brand,
		total_rental_income = 
			CASE
				WHEN discounted_rent >= 0 THEN giro_rent + rent - discounted_rent
				WHEN discounted_rent < 0 THEN giro_rent + rent + discounted_rent
				ELSE 0
			END,
		calculated = count_month / 12 * cte.gla * gla_income_mall,
		forgone_income = (count_month / 12 * cte.gla * gla_income_mall) - common_area_rent
	FROM cte_main cte
	LEFT JOIN giro_per_mall mall ON cte.portfolio_name = mall.portfolioname
								 AND cte.[year] = mall.[year]
	LEFT JOIN giro_per_brand brand ON cte.brand = brand.brand
								   AND cte.[year] = brand.[year]
	LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} portfolio ON cte.portfolio_id = portfolio.ID
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} t001k ON portfolio.WERKS = t001k.bwkey
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON t001k.bukrs = dim_comp.RobiKisaKod

)

SELECT
*,
ratio = CASE WHEN total_rental_income <> 0 then forgone_income / total_rental_income ELSE 0 END
FROM final
WHERE [year] > 2018
