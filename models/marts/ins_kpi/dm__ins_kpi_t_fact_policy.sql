{{
  config(
    materialized = 'table',tags = ['ins_kpi']
    )
}}

with dates as (
select
*
 from {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }}
 where [date] BETWEEN '2020-01-01' AND  GETDATE()
 )
,BloombergCurrenciesUSD_old AS (
	SELECT 
		[Date] = CONVERT(DATE, LEFT([Date], 2) + '/' + SUBSTRING([Date], 3, 2) + '/' + RIGHT([Date], 4), 104)
		,Currency 
		,CAST(Value1 AS decimal(18,6)) AS Value1
	FROM [RNSBI].[RNSBI].[dbo].tb146BloombergCurrency
	--WHERE Currency = 'RUB'
		)
,CurrencyTypes AS (
    SELECT DISTINCT Currency FROM BloombergCurrenciesUSD_old
),
combined as (
	SELECT ds.date, ct.Currency
    FROM dates ds
    CROSS JOIN CurrencyTypes ct
)
,comb_1 as (
SELECT
    c.date AS tarih,
    c.Currency,
	d.value1
FROM
    Combined c
LEFT JOIN
    BloombergCurrenciesUSD_old d ON c.date = d.[Date] AND c.Currency = d.Currency
)
,FilledValues AS (
    SELECT
        tarih,
        Currency,
        value1,
        MAX(CASE WHEN value1 IS NOT NULL THEN tarih ELSE NULL END) OVER (PARTITION BY Currency ORDER BY tarih ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_non_null_date
    FROM
        comb_1
)
,final_value as (
SELECT
    pd.tarih,
    pd.Currency,
    pd.value1,
        COALESCE(pd.value1, prev.value1, next.value1) AS final_value
FROM FilledValues pd
    OUTER APPLY (
        SELECT TOP 1 value1 FROM FilledValues p1
        WHERE p1.Currency = pd.Currency AND p1.tarih < pd.tarih AND p1.value1 IS NOT NULL
        ORDER BY p1.tarih DESC
    ) prev
    OUTER APPLY (
        SELECT TOP 1 value1 FROM FilledValues p2
        WHERE p2.Currency = pd.Currency AND p2.tarih > pd.tarih AND p2.value1 IS NOT NULL
        ORDER BY p2.tarih ASC
    ) next
	--WHERE Currency = 'RUB'
)
,BloombergCurrenciesUSD as (
select 
tarih as [Date],
Currency,
final_value as value1
 from final_value )
, 
CompanyUnionMappingTable AS 
(
	SELECT 
		company,
		[group],
		region = CASE WHEN region = 'NA' THEN 'CLO' ELSE region END,
		kyriba_company_description
	FROM (
	SELECT 
		KyribaKisaKod AS company,
		KyribaGrup AS [group],
		RegionCode AS region,
		Tanim AS kyriba_company_description
	FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} 
	) raw_data
)
SELECT
	
     [rls_region]
	,[rls_group]
	,[rls_company]
	,[rls_businessarea]= CONCAT (business_area,'_',(SELECT TOP 1 dimCompany.rls_region 
									FROM {{ ref('dm__dimensions_t_dim_companies') }} dimCompany
									WHERE dimCompany.KyribaKisaKod =  plc.company)
								)
	,insured_group_eng =(SELECT TOP 1 dimCompany.KyribaGrup 
						FROM {{ ref('dm__dimensions_t_dim_companies') }} dimCompany
						WHERE dimCompany.KyribaKisaKod =  plc.company)
	,insured_company_eng = (SELECT TOP 1 dimCompany.RobiKisaKod 
						FROM {{ ref('dm__dimensions_t_dim_companies') }} dimCompany
						WHERE dimCompany.KyribaKisaKod =  plc.company)
	,insured_company_description = (SELECT TOP 1 dimCompany.Tanim 
						FROM {{ ref('dm__dimensions_t_dim_companies') }} dimCompany
						WHERE dimCompany.KyribaKisaKod =  plc.company)
	,business_area
	,plc.[db_upload_timestamp]
	,plc.[broker]
	,plc.[insurer]
	,[policy_status] = NULL
	,plc.[additional_no]
	,[renewability_status] = NULL
	,[activeness_status] = NULL
	,plc.[start_date]
	,plc.[finish_date]
	,plc.[policy_no]
	,plc.[insured]
	,plc.[branch]
	,main_branch = (SELECT TOP 1 brc.main_branch 
						FROM {{ source('stg_ins_kpi', 'stg__ins_kpi_t_dim_branch') }} brc
						WHERE brc.branch = plc.branch)
	,branch_eng = (SELECT TOP 1 brc.branch_eng 
		FROM {{ source('stg_ins_kpi', 'stg__ins_kpi_t_dim_branch') }} brc
		WHERE brc.branch = plc.branch)
	,main_branch_eng = (SELECT TOP 1 brc.main_branch_eng
		FROM {{ source('stg_ins_kpi', 'stg__ins_kpi_t_dim_branch') }} brc
		WHERE brc.branch = plc.branch)
	,plc.[sum_insured]
	,plc.[gross_premium]
	,[currency] = 
				CASE 
					WHEN plc.[currency] = 'EURO' THEN 'EUR'
					WHEN plc.[currency] = 'TL' THEN 'TRY'
					WHEN plc.[currency] = 'POUND' THEN 'GBP'
					WHEN plc.[currency] = 'DZN' THEN 'DZD'
					ELSE plc.[currency]
				END 
	,plc.[risk_address]
	,plc.[explanation]
	,[sum_insured_in_eur] = 		
		CASE
			WHEN plc.[currency] = 'EURO' THEN sum_insured
			WHEN plc.[currency] = 'EUR' THEN sum_insured
			WHEN plc.[currency] = 'USD' THEN CAST(sum_insured / (SELECT TOP 1 b.Value1 FROM BloombergCurrenciesUSD b WHERE b.Currency = 'EUR' AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC) AS DECIMAL(18,2))
			WHEN plc.[currency] = 'TRY' THEN 
									CAST( 
										sum_insured / (
													(SELECT TOP 1 b.Value1
														FROM BloombergCurrenciesUSD b 
														WHERE 1=1 
															AND b.Currency = 'TRY' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC)
													/
													(SELECT TOP 1 1/b.Value1
														FROM BloombergCurrenciesUSD b
														WHERE 1=1
															AND b.Currency = 'EUR' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC) 
											) AS DECIMAL(18,2))	

			WHEN currency = 'MZN' THEN 
									CAST( 
										sum_insured / (
													(SELECT TOP 1 b.Value1
														FROM BloombergCurrenciesUSD b 
														WHERE 1=1 
															AND b.Currency = 'MZN' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC)
													/
													(SELECT TOP 1 1/b.Value1
														FROM BloombergCurrenciesUSD b
														WHERE 1=1
															AND b.Currency = 'EUR' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC) 
											) AS DECIMAL(18,2))		
			WHEN currency = 'GBP' THEN 
									CAST( 
										sum_insured / (
													(SELECT TOP 1 1/b.Value1 
														FROM BloombergCurrenciesUSD b 
														WHERE 1=1 
															AND b.Currency = 'GBP' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC)
													/
													(SELECT TOP 1 1/b.Value1
														FROM BloombergCurrenciesUSD b
														WHERE 1=1
															AND b.Currency = 'EUR' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC) 
											) AS DECIMAL(18,2))				
			WHEN currency = 'DZD' THEN 
									CAST(sum_insured / (
													(SELECT TOP 1 b.Value1 
														FROM BloombergCurrenciesUSD b 
														WHERE 1=1 
															AND b.Currency = 'DZD' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC)
													/
													(SELECT TOP 1 1/b.Value1
														FROM BloombergCurrenciesUSD b
														WHERE 1=1
															AND b.Currency = 'EUR' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC)
													) AS decimal(18,2))	
			END
	,[gross_premium_in_eur] =
		CASE
			WHEN currency = 'EUR' THEN gross_premium
			WHEN currency = 'USD' THEN CAST(gross_premium / (SELECT TOP 1 b.Value1 FROM BloombergCurrenciesUSD b WHERE b.Currency = 'EUR' AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC) AS DECIMAL(18,2))
			WHEN currency = 'TRY' THEN 
									CAST( 
										gross_premium / (
													(SELECT TOP 1 b.Value1 
														FROM BloombergCurrenciesUSD b 
														WHERE 1=1 
															AND b.Currency = 'TRY' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC)
													/
													(SELECT TOP 1 1/b.Value1
														FROM BloombergCurrenciesUSD b
														WHERE 1=1
															AND b.Currency = 'EUR' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC) 
											) AS DECIMAL(18,2))

			WHEN currency = 'MZN' THEN 
									CAST( 
										gross_premium / (
													(SELECT TOP 1 b.Value1
														FROM BloombergCurrenciesUSD b 
														WHERE 1=1 
															AND b.Currency = 'MZN' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC)
													/
													(SELECT TOP 1 1/b.Value1
														FROM BloombergCurrenciesUSD b
														WHERE 1=1
															AND b.Currency = 'EUR' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC) 
											) AS DECIMAL(18,2))		
			WHEN currency = 'GBP' THEN 
									CAST( 
										gross_premium / (
													(SELECT TOP 1 1/b.Value1 
														FROM BloombergCurrenciesUSD b 
														WHERE 1=1 
															AND b.Currency = 'GBP' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC)
													/
													(SELECT TOP 1 1/b.Value1
														FROM BloombergCurrenciesUSD b
														WHERE 1=1
															AND b.Currency = 'EUR' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC) 
											) AS DECIMAL(18,2))				
			WHEN currency = 'DZD' THEN 
									CAST(gross_premium / (
													(SELECT TOP 1 b.Value1 
														FROM BloombergCurrenciesUSD b 
														WHERE 1=1 
															AND b.Currency = 'DZD' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC)
													/
													(SELECT TOP 1 1/b.Value1
														FROM BloombergCurrenciesUSD b
														WHERE 1=1
															AND b.Currency = 'EUR' 
															AND b.[Date] <= plc.[start_date] ORDER BY b.[Date] DESC)
													) AS decimal(18,2))	
			END
	FROM {{ source('stg_ins_kpi', 'stg__ins_kpi_t_fact_policy') }} plc
		LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dimCompanies ON plc.company = dimCompanies.KyribaKisaKod
	WHERE 1=1 
		AND plc.[start_date] >= '2019-01-01'
