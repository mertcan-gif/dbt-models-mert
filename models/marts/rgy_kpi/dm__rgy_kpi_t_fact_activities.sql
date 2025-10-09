{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

WITH CTE AS (
    SELECT 
      id,
      [PortfolioName],
      [CategoryName],
      [LeaseTypeName],
      [BrandName],
      [LeaseStartDate],
      [LeaseEndDate]
    FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_incomeapprovalformlist') }}
),

DatesExpanded AS (
    SELECT 
      id,
      [PortfolioName] AS portfolio_name,
      [CategoryName] AS category_name,
      [LeaseTypeName] AS lease_type_name,
      [BrandName] AS brand,
      CAST([LeaseStartDate] AS DATE) AS lease_start_date,
      CAST([LeaseEndDate]AS DATE) AS lease_end_date,
      CONVERT(date, DATEADD(DAY, n, LeaseStartDate)) AS [date]
    FROM CTE
    CROSS APPLY (
		SELECT TOP (CASE
						WHEN DATEDIFF(DAY, LeaseStartDate, LeaseEndDate) >= 0 THEN DATEDIFF(DAY, LeaseStartDate, LeaseEndDate) + 1
						ELSE 0 
					 END)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM master.dbo.spt_values
    ) AS Numbers
)

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(p.WERKS, '_', rls_region),
	DatesExpanded.*,
  p.WERKS AS business_area,
  t001W.NAME1 AS business_area_description,
	f.[Count] AS footfall
FROM DatesExpanded
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} p ON DatesExpanded.portfolio_name = p.[name]
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_footfall') }} f ON p.id = f.PortfolioID
												 AND DatesExpanded.[date] = f.[Date]
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} t001k ON p.werks = t001k.bwkey
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON t001k.bukrs = dim_comp.RobiKisaKod
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001W ON p.WERKS = t001W.WERKS