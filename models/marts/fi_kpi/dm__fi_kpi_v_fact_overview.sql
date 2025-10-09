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
),

overview_aggregated AS 
(
	SELECT
		RobiKisaKod
		,Tanim 
		,YK_SEKTOR
		,TARIH  AS [date]
		,RTCUR as unit
		,SUM(ANLIKBAKIYE_IPB) AS value
		,SUM(ANLIKBAKIYE_USD) AS value_in_usd
		,SUM(ANLIKBAKIYE_EUR) AS value_in_eur
	FROM 
		(
		SELECT
			RobiKisaKod
			,Tanim 
			,YK_SEKTOR
			,TARIH
			,RTCUR
			,ANLIKBAKIYE_IPB 
			,ANLIKBAKIYE_USD 
			,ANLIKBAKIYE_EUR 
			FROM {{ ref('dm__nwc_kpi_t_fact_s4mevduat') }} xyz
				LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} company_info ON xyz.RBUKRS collate database_default = company_info.RobiKisaKod
			WHERE TARIH = CAST(DATEADD(DAY,-1,GETDATE()) AS DATE)
		) tb049MevduatXYZ_eur_added
	GROUP BY 
		RobiKisaKod
		,Tanim 
		,YK_SEKTOR
		,TARIH
		,RTCUR
)

SELECT
	[rls_region] = (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = overview_aggregated.RobiKisaKod)
	,[rls_group] =
		CONCAT(
				COALESCE((SELECT TOP 1 [group] FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = overview_aggregated.RobiKisaKod),'')
				,'_'
				,COALESCE((SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = overview_aggregated.RobiKisaKod),'')
				) 
	, [rls_company] =
			CONCAT(
				COALESCE((SELECT TOP 1 RobiKisaKod FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = overview_aggregated.RobiKisaKod),'')
				,'_'
				,COALESCE((SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = overview_aggregated.RobiKisaKod),'')
				) 
    ,[rls_businessarea] = NULL
	,(SELECT MAX(TARIH) FROM [RNSBI].[RNSBI].[dbo].[tb049MevduatXYZ]) AS [data_entry_timestamp]
	,RobiKisaKod AS [company_code]
	,[Tanim] AS [company]
	,[YK_SEKTOR] AS [industry]
	,'total_cash' AS  [type]
	,[date]
	,[value]
	,[unit]
	,[value_in_usd]
	,[value_in_eur]
FROM overview_aggregated
