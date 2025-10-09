{{
  config(
    materialized = 'view',tags = ['eff_kpi']
    )
}}
WITH NON_RLS_DATA AS (
	SELECT
		KyribaGrup AS [group],
		KyribaKisaKod AS [company],
		BANKA AS [bank_description],
		RACCT AS [account_number],
		RTCUR AS [currency],
		CAST([Minumum Bakiye] AS decimal(18, 2)) AS [minimum_balance],
		CAST(
			[Ortalama Bakiye] AS decimal(18, 2)
		) AS [average_balance],
		CAST (
			(
				SELECT TOP (1) ANLIKBAKIYE_IPB
				FROM {{ ref('dm__nwc_kpi_t_fact_s4mevduat') }} AS m 
				WHERE (TARIH = CONVERT(date, GETDATE()))
					AND (RACCT = A.RACCT)
			) AS decimal(18, 2)
		) AS [current_balance]
	FROM (
			SELECT kc.KyribaGrup,
				kc.KyribaKisaKod,
				MIN(mevduat.BANKATANIMI) as BANKA,
				mevduat.RACCT,
				mevduat.RTCUR,
				AVG(mevduat.ANLIKBAKIYE_IPB) AS 'Ortalama Bakiye',
				CASE
					WHEN RTCUR = 'TRY'
					AND AVG(ANLIKBAKIYE_IPB) < 1000000 THEN 1
					WHEN RTCUR = 'EUR'
					AND AVG(ANLIKBAKIYE_IPB) < 100000 THEN 1
					WHEN RTCUR = 'USD'
					AND AVG(ANLIKBAKIYE_IPB) < 100000 THEN 1
					WHEN RTCUR = 'RUB'
					AND AVG(ANLIKBAKIYE_IPB) < 10000000 THEN 1
					ELSE 0
				END AS 'CheckBalance',
				MIN(mevduat.ANLIKBAKIYE_IPB) AS 'Minumum Bakiye',
				CASE
					WHEN RTCUR = 'TRY'
					AND MIN(ANLIKBAKIYE_IPB) < 1000000 THEN 1
					WHEN RTCUR = 'EUR'
					AND MIN(ANLIKBAKIYE_IPB) < 100000 THEN 1
					WHEN RTCUR = 'USD'
					AND MIN(ANLIKBAKIYE_IPB) < 100000 THEN 1
					WHEN RTCUR = 'RUB'
					AND MIN(ANLIKBAKIYE_IPB) < 10000000 THEN 1
					ELSE 0
				END AS 'CheckBalanceMIN'
			FROM {{ ref('dm__nwc_kpi_t_fact_s4mevduat') }} AS mevduat
				LEFT OUTER JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} AS kc ON mevduat.RBUKRS = kc.RobiKisaKod COLLATE DATABASE_DEFAULT
			WHERE (mevduat.NONSAP IN ('S'))
				AND (
					mevduat.HESAP_TIPI_TANIMI IN ('Vadesiz', N'Tanımlı Değil')
				)
				AND (mevduat.RTCUR IN ('TRY', 'EUR', 'USD', 'RUB'))
				AND (
					mevduat.TARIH BETWEEN DATEADD(
						month,
						- 1,
						CONVERT(date, GETDATE())
					)
					AND CONVERT(date, GETDATE())
				)
			GROUP BY kc.KyribaGrup,
				kc.KyribaKisaKod,
				mevduat.BANKATANIMI,
				mevduat.RTCUR,
				mevduat.RACCT
		) AS A
	WHERE (A.[CheckBalance] = 0)
		OR (A.[CheckBalanceMIN] = 0)
)

, CompanyUnionMappingTable AS 
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
	[rls_region] = (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = NON_RLS_DATA.[Company] COLLATE DATABASE_DEFAULT),
    [rls_group] = CONCAT(
							(SELECT TOP 1 [group] FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = NON_RLS_DATA.[Company] COLLATE DATABASE_DEFAULT)
							,'_',
							(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = NON_RLS_DATA.[Company] COLLATE DATABASE_DEFAULT) 
						),
    [rls_company] = CONCAT(NON_RLS_DATA.[Company],'_',(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = NON_RLS_DATA.[Company] COLLATE DATABASE_DEFAULT) COLLATE DATABASE_DEFAULT ),
    [rls_businessarea] = NULL,
    *
FROM NON_RLS_DATA
