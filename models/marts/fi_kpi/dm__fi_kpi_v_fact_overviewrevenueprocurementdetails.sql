{{
  config(
    materialized = 'view',tags = ['fi_kpi']
    )
}}

WITH CompanyUnionMappingTable AS 
(
	SELECT 
		company,
		[group],
		region = CASE WHEN region = 'NA' THEN 'CLO' ELSE region END
	FROM (
	SELECT KyribaKisaKod AS company,KyribaGrup AS [group],RegionCode AS region FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }}
	) raw_data
)

SELECT 
	[rls_region] = (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = fcd.[entity] COLLATE database_default)
	,[rls_group] =
		CONCAT(
				COALESCE((SELECT TOP 1 [group] FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = fcd.[entity] COLLATE database_default),'')
				,'_'
				,COALESCE((SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = fcd.[entity] COLLATE database_default),'')
				) 
	,[rls_company] = 
		CONCAT(
			COALESCE([entity] COLLATE database_default,'')
			,'_'
			,COALESCE((SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = fcd.[entity] COLLATE database_default),'')
			)

	,[rls_businessarea] = NULL
	,[year_month]
	,[entity]
	,[industry]
	,[revenue_ar_eur] AS  [revenue_eur]
	,[revenue_finance_sector_operations_ar_eur] AS [revenue_finance_sector_operations_eur]
	,[total_assets_cr_eur] AS [total_assets_eur]
	,[current_liabilities_cr_eur] AS [current_liabilities_eur]
	,[noncurrent_liabilities_cr_eur] AS [noncurrent_liabilities_eur]
	,[equity_cr_eur] AS [equity_eur]
	,[material_costs_ar_eur] AS [material_costs_eur] 
	,[services_costs_ar_eur] AS [services_costs_eur]
	,[personnel_expenses_ar_eur] AS [personnel_expenses_eur]
	,[cost_of_goods_sold_ar_eur] AS [cost_of_goods_sold_eur]
	,[machinery_equipment_and_other_rent_expenses_ar_eur] AS [machinery_equipment_and_other_rent_expenses_eur]
	,[other_costs_ar_eur] AS [other_costs_eur]
	,[cost_of_finance_sector_operations_ar_eur] AS [cost_of_finance_sector_operations_eur]
FROM {{ source('stg_fc_kpi', 'raw__fc_kpi_t_fact_fcdetails') }} fcd