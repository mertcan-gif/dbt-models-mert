{{
  config(
    materialized = 'view',tags = ['eff_kpi']
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
	[rls_region] = (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = vh.[Şirket Kodu] COLLATE DATABASE_DEFAULT)
    ,[rls_group] = CONCAT(
							(SELECT TOP 1 [group] FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = vh.[Şirket Kodu] COLLATE DATABASE_DEFAULT)
							,'_',
							(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = vh.[Şirket Kodu] COLLATE DATABASE_DEFAULT) 
						)
    ,[rls_company] = CONCAT(vh.[Şirket Kodu],'_',(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = vh.[Şirket Kodu] COLLATE DATABASE_DEFAULT) COLLATE DATABASE_DEFAULT )
    ,[rls_businessarea] = NULL
	,[Kyriba Grubu] AS [kyriba_group]
	,[Şirket Kodu] AS [company_code]
	,[Banka Tanımı] AS [bank_description]
	,[RACCT] AS [account_number]
	,[PB] AS [currency]
	,[Minumum Bakiye] AS [minimum_balance]
	,[Ortalama Bakiye] AS [average_balance]
	,[Anlık Bakiye] AS [current_balance]
	,[CreatedDate] AS [created_date]
 FROM {{ source('stg_eff_kpi', 'raw_eff_kpi_t_fact_demanddeposittrhistorical') }} vh
