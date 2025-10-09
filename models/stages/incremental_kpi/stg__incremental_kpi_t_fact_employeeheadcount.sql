{{
  config(
    materialized = 'incremental',tags = ['incremental_kpi']
    )
}}
 
WITH grouped_data AS (
SELECT 
    rls_region
    ,rls_group
    ,rls_company
    ,rls_businessarea
    ,company_code
    ,company_name
    ,actual_working_country
    ,[employment_type_tr]
    ,[collar_type]
    ,[gender]
    ,COUNT(DISTINCT [sf_id_number]) AS employee_count
    ,snapshot_date = CAST(GETDATE() AS date)
    --,source
  FROM {{ ref('stg__hr_kpi_t_dim_employeelist') }}
  GROUP BY 
    rls_region
    ,rls_group
    ,rls_company
    ,rls_businessarea
    ,company_code
    ,company_name
    ,actual_working_country
    ,[collar_type]
    ,[gender]
    ,[employment_type_tr]
    --,source
	)

,rls_manipulation AS (
	SELECT 
		CASE
			WHEN company_code = 'Ballast Europe' THEN 'EUR' 
			WHEN (rls_region IS NULL OR rls_region = 'NA' and company_code <> 'BBN') THEN 'TUR'
			WHEN company_code = 'BBN' THEN 'EUR'
			ELSE rls_region
		END AS rls_region
		,CASE
			WHEN company_code = 'Ballast Europe' THEN 'BNGROUP_EUR'
			WHEN company_code IN ('RIC', 'PMQ') THEN CONCAT(rls_group, 'TUR')
			WHEN company_code = 'OZB' THEN 'RETGROUP_TUR'
			WHEN company_code = 'RTS' THEN 'RETGROUP_TUR'
			WHEN company_code = 'RIC' THEN 'NONGR_TUR'
			WHEN rls_group IS NULL THEN CONCAT(rls_group, '_', 'TUR')
			WHEN (rls_region = 'NA' AND company_code <> 'BBN') THEN REPLACE(rls_group,'NA','TUR')
			WHEN company_code = 'BBN' THEN REPLACE(rls_group,'NA','EUR')
			ELSE rls_group
		END AS rls_group
		,CASE
			WHEN company_name = 'Ballast Europe' THEN 'NS_BLN_EUR'
			WHEN company_code = 'OZB' THEN 'OZS_TUR'
			WHEN company_code = 'RTS' THEN 'NS_RTS_TUR' --THEN CONCAT(company_code, '_', 'TUR')
			WHEN company_code IN ('RIC', 'PMQ') THEN CONCAT(company_code, '_', 'TUR')
			WHEN (rls_region = 'NA' AND company_code <> 'BBN') THEN CONCAT(company_code, '_', 'TUR')
			WHEN (rls_region = 'NA' AND company_code = 'BBN') THEN CONCAT(company_code, '_', 'EUR')
			ELSE rls_company
		END AS rls_company
		,CASE
			WHEN company_name = 'Ballast Europe' THEN '_EUR'
			WHEN company_code IN ('RIC', 'PMQ') THEN '_TUR'
			WHEN (rls_region = 'NA' AND company_code <> 'BBN') THEN '_TUR'
			WHEN (rls_region = 'NA' AND company_code = 'BBN') THEN '_EUR'
			WHEN rls_region IS NULL THEN '_TUR'
			ELSE rls_businessarea
		END AS rls_businessarea
		,company_code
		,company_name
		,actual_working_country
		,employment_type_tr
		,collar_type
		,gender
		,employee_count
		,snapshot_date
	FROM grouped_data
	)
	SELECT 
		CASE
			WHEN rls_group IN ('CLOSED_EUR', 'RETGROUP_EUR', 'RTIGROUP_LBY') THEN 'TUR'
			ELSE rls_region
		END AS rls_region
		,CASE
			WHEN rls_group IN ('CLOSED_EUR', 'RETGROUP_EUR', 'RTIGROUP_LBY') THEN 'HOLDING_TUR'
			ELSE rls_group
		END AS rls_group
		,CASE
			WHEN rls_group IN ('CLOSED_EUR', 'RETGROUP_EUR', 'RTIGROUP_LBY') THEN 'HOL_TUR'
			ELSE rls_company
		END AS rls_company
		,CASE
			WHEN rls_group IN ('CLOSED_EUR', 'RETGROUP_EUR', 'RTIGROUP_LBY') THEN '_TUR'
			ELSE rls_businessarea
		END AS rls_businessarea
	,[company_code]
	,[company_name]
	,[actual_working_country]
	,[employment_type_tr] AS employment_type
	,[collar_type]
	,[gender]
	,employee_count
	,[snapshot_date] 
FROM rls_manipulation


{% if is_incremental() %}
	WHERE CONVERT(DATE, GETDATE()) > 
			(SELECT MAX([snapshot_date]) FROM {{ this }})
{% endif %}