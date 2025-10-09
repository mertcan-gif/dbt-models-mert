{{
  config(
    materialized = 'table',tags = ['eff_kpi']
    )
}}

WITH DateRange AS (
    SELECT 
        [ID],
        [IsActive],
        [IsDeleted],
        [Rowguid],
        [CreatedAt],
        [CreatedBy],
        [Creator],
        [UpdatedAt],
        [UpdatedBy],
        [Updater],
        [OperationID],
        [SiteID],
        [ERV],
        [ERVCurrencyID],
        [VersionValidFrom],
        [VersionValidTo],
        [SectorID],
        [ApprovalStatus],
        [DisplayIndex],
        CAST([ValidFrom] AS DATE) AS [ValidFrom],
        CAST([ValidUntil] AS DATE) AS [ValidUntil]
    FROM  {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_erv') }}
    WHERE 1=1
	AND [ERVCurrencyID]='3' 
	AND SiteId IS NOT NULL
)
, ExpandedDates AS (
    SELECT 
        dr.[ID],
        dr.[IsActive],
        dr.[IsDeleted],
        dr.[Rowguid],
        dr.[CreatedAt],
        dr.[CreatedBy],
        dr.[Creator],
        dr.[UpdatedAt],
        dr.[UpdatedBy],
        dr.[Updater],
        dr.[OperationID],
        dr.[SiteID],
        dr.[ERV],
        dr.[ERVCurrencyID],
        dr.[VersionValidFrom],
        dr.[VersionValidTo],
        dr.[SectorID],
        dr.[ApprovalStatus],
        dr.[DisplayIndex],
        dr.[ValidFrom],
        DATEADD(DAY, n, dr.ValidFrom) AS [TransactionDate],
        dr.[ValidUntil]
    FROM DateRange dr
    CROSS APPLY (
        SELECT TOP (DATEDIFF(DAY, dr.ValidFrom, dr.ValidUntil) + 1)
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM master.dbo.spt_values
    ) AS Numbers
),

transformed_dates_cte as (
	SELECT 
    [ID],
    [IsActive],
    [IsDeleted],
    [Rowguid],
    [CreatedAt],
    [CreatedBy],
    [Creator],
    [UpdatedAt],
    [UpdatedBy],
    [Updater],
    [OperationID],
    [SiteID],
    [ERV],
    [ERVCurrencyID],
    [VersionValidFrom],
    [VersionValidTo],
    [SectorID],
    [ApprovalStatus],
    [DisplayIndex],
    [ValidFrom],
    [TransactionDate] AS [GeneratedDate], -- Daily date generated between ValidFrom and ValidUntil
    [ValidUntil]
FROM ExpandedDates
WHERE day([TransactionDate])=01
),

dedup as (
select *,
	rn = row_number() OVER (PARTITION BY SiteId,GeneratedDate ORDER BY GeneratedDate)
from transformed_dates_cte
),

final_date AS (
	SELECT * 
	FROM dedup 
	WHERE rn=1
),

final_cte AS (
SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(portfolio.werks, '_', rls_region),
	portfolio_name = ocr.PortfolioName,
	ocr.brand,
    ocr.RGYMainSector AS main_sector,
	ocr.SiteGLA AS m2,
	ocr.ContractStartDate AS contract_start_date,
	ocr.ContractEndDate AS contract_end_date,
	[year],
	GiroRate AS giro_rate,
	TotalGiro AS total_turnover,
	GiroRent AS turnover_rent,
	rent,
	rentdiscount AS discounted_rent,
	CAMType AS cam_type,
	total_rental_income =
	CASE
		WHEN RentDiscount >= 0 THEN GiroRent + Rent - RentDiscount
		WHEN RentDiscount < 0 THEN GiroRent + Rent + RentDiscount
		ELSE 0
	END,
	erv
FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_ocrreport') }} ocr 
	LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_contractmanagementlist') }} cm ON ocr.contractcode = cm.contractcode AND ocr.sitename = cm.sitename
	LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_term') }} tr ON ocr.termid = tr.id
	LEFT JOIN final_date ON ocr.StartDate=final_date.[GeneratedDate] AND cm.SiteId=final_date.[SiteID]
    LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} portfolio ON ocr.portfolioid = portfolio.ID
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} t001k ON portfolio.WERKS = t001k.bwkey
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON t001k.bukrs = dim_comp.RobiKisaKod
WHERE ocr.SiteTypeName = N'Dükkan'
)

SELECT 
*,
[difference] = total_rental_income - erv
FROM final_cte
WHERE [year] > 2018