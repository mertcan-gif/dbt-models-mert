{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

WITH erv AS (
    SELECT 
        t.SiteID,
        t.ERV,
        t.ERVCurrencyID,
        t.ValidFrom,
        t.ValidUntil
    FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_erv') }} t
    WHERE 
        t.SiteID IS NOT NULL
        AND t.IsActive = '1'
        AND t.ERVCurrencyID = '3'
),
 
dates AS (
    SELECT DATEADD(MONTH, number, '2018-01-31') AS first_date
    FROM master.dbo.spt_values
    WHERE type = 'P' AND number BETWEEN 0 AND 100
),
 
main_cte AS (
SELECT
    P.id AS portfolio_id,
	p.[Name] as portfolio_name,
	s.id as site_id,
	s.[Name] as site_name,
    [SiteType].[id] AS site_type_id,
	[SiteType].[Name] as site_type_name,
    CAST(D.first_date AS date) AS first_date,
	ISNULL(ValidContract.GLAsquaremeter, s.GLA) squaremeter,
		 CASE
			WHEN s.DisposalDate IS NOT NULL AND s.DisposalDate < first_date THEN N'Yok edildi'
			WHEN ValidContract.MTDate IS NULL OR  ValidContract.MTDate > first_date THEN N'Teslim Edilmedi'
			WHEN (ValidContract.MTDate IS NOT NULL AND ValidContract.MTDate <= first_date) AND (ValidContract.MADate IS NULL OR (ValidContract.MADate > first_date)) THEN N'Teslim Edildi'
			WHEN (ValidContract.MADate IS NOT NULL AND (ValidContract.MADate <= first_date)) AND ( ValidContract.MKDate IS NULL OR  ValidContract.MKDate > first_date) THEN N'Açık'
			WHEN  ValidContract.MKDate IS NOT NULL AND ValidContract.MKDate <= first_date THEN N'Kapalı'
			ELSE N'Boş'
		END AS PhysicalSiteState,
		CASE
			WHEN s.DisposalDate IS NOT NULL THEN 'Pasif'
			WHEN ValidContract.ContractID IS NULL THEN N'Boş'
			ELSE
				CASE
					WHEN ValidContract.MKDate IS NOT NULL AND ValidContract.MKDate <= first_date THEN N'Boş'
					WHEN ValidContract.MADate IS NOT NULL AND ValidContract.MADate <= first_date THEN N'Dolu'
					WHEN ValidContract.MTDate IS NOT NULL AND ValidContract.MTDate <= first_date THEN N'Sözleşme İmza'
					WHEN DocumentExistance.DocumentExists = 1 AND DocumentExistance.CreatedAt <= first_date THEN N'Sözleşme İmza'
					ELSE N'Boş'
				END
		 END AS SiteStatus
 
FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_site') }} s
    JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} P ON s.PortfolioID = P.ID
    JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_sitetype') }} SiteType ON s.TypeID = SiteType.ID
    CROSS JOIN dates D
 
    OUTER APPLY(
        SELECT TOP 1
            ContractList.*, squaremeter.GLAsquaremeter
        FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_sitecontractlist') }} ContractList
        LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_activesquaremeterlist') }} squaremeter
            ON s.ID = squaremeter.SiteID
            AND ContractList.ContractID = squaremeter.ContractID
        WHERE
            ContractList.SiteID = s.ID
            AND (ContractList.MKDate IS NULL OR ContractList.MKDate >= D.first_date)
            AND ContractList.ContractState NOT IN (0, 14, 15)
        ORDER BY ContractList.MKDate ASC, ContractList.ContractStartDate DESC, ContractList.MADate DESC, ContractList.ContractID DESC
    ) ValidContract
 
		OUTER APPLY(
			SELECT * FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_contractdocumentexistance') }} dl
			WHERE
				dl.ContractID = ValidContract.ContractID
				AND dl.SiteID = s.ID
		) DocumentExistance
		WHERE
			(s.IsVirtual IS NULL OR s.IsVirtual = 0)
			AND s.IsActive = 1 AND s.IsDeleted = 0
			AND (s.CreationDate IS NULL OR s.CreationDate < first_date)
            AND SiteType.[Name] = N'Dükkan'
			AND P.ID IN ('1', '2', '4', '5', '8', '9', '10', '11', '14', '15', '17', '23')
)

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(p.werks, '_', rls_region),
	first_date AS [date],
	portfolio_name,
	gross_leasable_area = SUM(CASE WHEN sitestatus IN (N'Boş', N'Dolu', N'Sözleşme İmza') THEN main_cte.squaremeter ELSE NULL END),
	occupied_site_count = COUNT(CASE WHEN sitestatus IN (N'Dolu', N'Sözleşme İmza') THEN site_name ELSE NULL END),
	occupied_site_area_in_sqm = SUM(CASE WHEN sitestatus IN (N'Dolu', N'Sözleşme İmza') THEN main_cte.squaremeter ELSE NULL END),
	unoccupied_site_count = COUNT(CASE WHEN sitestatus IN (N'Boş') THEN site_name ELSE NULL END),
	unoccupied_site_area_in_sqm = SUM(CASE WHEN sitestatus IN (N'Boş') THEN main_cte.squaremeter ELSE NULL END),
	physical_occupancy = (SUM(CASE WHEN sitestatus IN (N'Dolu') THEN main_cte.squaremeter ELSE NULL END) / NULLIF(SUM(CASE WHEN sitestatus IN (N'Boş', N'Dolu', N'Sözleşme İmza') THEN main_cte.squaremeter ELSE NULL END), 0)),
	financial_occupancy = (SUM(CASE WHEN sitestatus IN (N'Dolu', N'Sözleşme İmza') THEN main_cte.squaremeter ELSE NULL END) / NULLIF(SUM(CASE WHEN sitestatus IN (N'Boş', N'Dolu', N'Sözleşme İmza') THEN main_cte.squaremeter ELSE NULL END), 0)),
	financial_loss = (SUM(CASE WHEN sitestatus IN (N'Boş') THEN ERV ELSE NULL END) / NULLIF(SUM(CASE WHEN sitestatus IN (N'Boş', N'Dolu', N'Sözleşme İmza') THEN ERV ELSE NULL END), 0))
FROM main_cte
LEFT JOIN erv ON CAST(main_cte.first_date AS date) BETWEEN CAST(erv.ValidFrom AS date) AND CAST(erv.ValidUntil as date)
				AND main_cte.site_id = erv.SiteID
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} p ON main_cte.portfolio_id = p.ID
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }}  dim_comp ON p.BusinessArea = dim_comp.RobiKisaKod
WHERE CAST(first_date AS DATE)<=CAST(GETDATE() AS DATE)
GROUP BY
	rls_region,
	rls_group,
	rls_company,
	CONCAT(p.werks, '_', rls_region),
	first_date,
	portfolio_id,
	portfolio_name
