{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

WITH cte_main AS (
	SELECT 
		PortfolioID,
		PortfolioName,
		Brand,
		SiteName,
		SiteTypeName,
		[year],
		term.id AS month_id,
		term.[name] AS [month],
		RGYMainSector,
		RGYSectorLVL1,
		SUM(SiteGla) AS m2,
		days_m2 = SUM(ActiveDays) * SUM(SiteGLA),
		SUM(TotalGiro) AS total_amount
	FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_ocrreport') }} ocr
	LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_term') }} term ON ocr.TermID = term.id
	WHERE 1=1
		AND SiteTypeName IN (N'Dükkan', N'Kiosk')
	GROUP BY 
		PortfolioID,
		PortfolioName,
		Brand,
		SiteName,
		SiteTypeName,
		[Year],
		term.id,
		term.[name],
		RGYMainSector,
		RGYSectorLVL1
),

lfl AS (
	SELECT 
		c1.PortfolioID AS portfolio_id,
		c1.PortfolioName AS portfolio_name,
		c1.brand,
		c1.SiteName AS site_name,
		c1.SiteTypeName AS site_type_name,
		c1.RGYMainSector AS main_sector,
		c1.RGYSectorLVL1 as sub_sector,
		c1.[year],
		c1.month_id,
		c1.[month],
		c1.m2,
		c1.days_m2 AS days_m2,
		c1.total_amount AS total_giro,
		lfl = 
			CASE
				WHEN c1.days_m2 = c2.days_m2 AND c1.total_amount <> 0 AND c2.total_amount <> 0 THEN 'LFL'
				ELSE 'NON-LFL'
			END
	FROM  cte_main c1
	LEFT JOIN cte_main c2 ON c1.PortfolioName = c2.PortfolioName
						  AND c1.Brand = c2.Brand
						  AND c1.SiteName = c2.SiteName
						  AND c1.[month] = c2.[month]
						  AND c1.RGYMainSector = c2.RGYMainSector
						  AND c1.RGYSectorLVL1 = c2.RGYSectorLVL1
						  AND c1.[Year] = (c2.[Year] + 1)
),

-- monthly_visitor AS (
-- 	SELECT
-- 		portfolioid,
-- 		YEAR([date]) AS [year],
-- 		FORMAT([date], 'MMMM', 'tr-TR') AS [month],
-- 		SUM([count]) AS footfall
-- 	FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_footfall') }}
-- 	GROUP BY 
-- 		portfolioid,
-- 		YEAR([date]),
-- 		FORMAT([date], 'MMMM', 'tr-TR')
-- ),

cpi AS (
	SELECT 
		DISTINCT
		termid,
		cpiyear
	FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_priceindex') }}
	WHERE CPIYear is not null
),

LatestDates AS (
    SELECT 
        date_value,
        MAX(date_value) OVER (PARTITION BY YEAR(date_value), MONTH(date_value)) AS max_date
    FROM {{ ref('stg__dimensions_t_dim_dailys4currencies') }}
    WHERE currency = 'TRY'
),

currencies AS(
    SELECT 
        YEAR(c.date_value) AS [year],
        FORMAT(c.date_value, 'MMMM', 'tr-TR') AS [month],
        c.try_value,
        c.usd_value,
        c.eur_value
    FROM {{ ref('stg__dimensions_t_dim_dailys4currencies') }} c
    JOIN LatestDates ld ON c.date_value = ld.max_date
                        AND c.date_value = ld.date_value
    WHERE c.currency = 'TRY'
)

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(t001k.bwkey, '_', rls_region),
	f.portfolio_name,
	f.brand,
	f.site_name,
	f.site_type_name,
	f.main_sector,
	f.sub_sector,
	f.[year],
	f.[month],
	f.m2,
	f.days_m2,
	try_total_giro = f.total_giro,
	eur_total_giro = f.total_giro * currencies.eur_value,
	usd_total_giro = f.total_giro * currencies.usd_value,
	cpiyear AS cpi_year,
	-- footfall,
	f.lfl
FROM lfl f
-- LEFT JOIN monthly_visitor mv ON f.portfolio_id = mv.PortfolioID
-- 							 AND f.[year] = mv.[year]
-- 							 AND f.[month] = mv.[month]
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} portfolio ON f.portfolio_id = portfolio.ID
LEFT JOIN cpi ON f.month_id = cpi.termid
LEFT JOIN currencies ON f.[year] = currencies.[year] 
					 AND f.[month] = currencies.[month]
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} t001k ON portfolio.WERKS = t001k.bwkey
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON t001k.bukrs = dim_comp.RobiKisaKod
