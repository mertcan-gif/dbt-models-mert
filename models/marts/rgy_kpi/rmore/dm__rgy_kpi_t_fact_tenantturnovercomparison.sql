{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

WITH nebim AS (
SELECT
	contract_code = ContractCode,
	site_code = SiteCode,
	[year] = YEAR(SalesDate),
	[month] = MONTH(SalesDate),
	nebim_giro_try = SUM(NetSales)
	-- nebim_transaction_count = SUM(TransactionCount)
FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_dailysale') }} ds
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_nebimcontractsite') }} cs ON ds.ContractSiteID = cs.ID
GROUP BY 
YEAR(SalesDate),
MONTH(SalesDate),
ContractCode,
SiteCode
),

iciro AS (
SELECT 
	contract_code = ContractCode,
	site_code = SiteCode,
	receipt_month = MONTH(ReceiptDate),
	receipt_year = YEAR(ReceiptDate),
	iciro_giro_try =  (Amount * [Count]) / ((KDV + 100) / 100)
	-- iciro_transaction_count = COUNT(ReceiptDate)
FROM "aws_stage"."rgy_kpi"."raw__rgy_kpi_t_fact_getreceiptproductreport" gr
LEFT JOIN "aws_stage"."rgy_kpi"."raw__rgy_kpi_t_dim_girocontractsite" cs ON gr.ContractSiteID = cs.ID
),

iciro_sum AS (
SELECT
	contract_code,
	site_code,
	receipt_month,
	receipt_year,
	SUM(iciro_giro_try) AS iciro_giro_try
FROM iciro
GROUP BY
	contract_code,
	site_code,
	receipt_month,
	receipt_year
)

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(p.WERKS, '_', rls_region),
	ocr.PortfolioName AS portfolio_name,
	ocr.Brand AS brand,
	ocr.ContractCode AS contract_code,
	ocr.SiteName AS site_code,
	ocr.SiteGLA AS gla,
	ocr.[year],
	MONTH(ocr.StartDate) AS [month],
	-- nebim_transaction_count,
	nebim_giro_try,
	-- iciro_transaction_count,
	iciro_giro_try,
	ocr.TotalGiro AS total_giro,
	nebim_deviation = nebim_giro_try - ocr.TotalGiro,
	nebim_deviation_rate = 
		(CASE 
			WHEN nebim_giro_try <> 0 THEN ((nebim_giro_try - ocr.TotalGiro) / nebim_giro_try)
			ELSE NULL
		END) * 100,
	iciro_deviation = iciro_giro_try - ocr.TotalGiro,
	iciro_deviation_rate =
		(CASE
			WHEN iciro_giro_try <> 0 THEN ((iciro_giro_try - ocr.TotalGiro) / iciro_giro_try)
			ELSE NULL
		END) * 100
FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_ocrreport') }} ocr 
LEFT JOIN nebim ON ocr.ContractCode = nebim.contract_code 
				AND ocr.SiteName = nebim.site_code
				AND ocr.[year] = nebim.[year]
				AND MONTH(StartDate) = nebim.[month]
LEFT JOIN iciro_sum ON ocr.ContractCode = iciro_sum.contract_code
				AND ocr.SiteName = iciro_sum.site_code
				AND ocr.[year] = receipt_year
				AND MONTH(StartDate) = receipt_month
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} p ON ocr.PortfolioID = p.ID
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON p.BusinessArea = dim_comp.RobiKisaKod
