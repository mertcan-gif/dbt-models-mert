{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
WITH CLAIM_DATES AS (

	SELECT DISTINCT
		business_area
		,claim_year
		,claim_mto_scope
	FROM {{source('stg_to_kpi','raw__to_kpi_t_fact_claimdata')}}
),

RAW_DATA AS (
	SELECT 
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,project_id
		,[Total Number of Concluded Claims] = CAST(SUM(requests_resolved) AS nvarchar(255)) 
		,[claim_year] = CAST(claim_year AS nvarchar(255))
		,[Inclusion of Claim Processes in MTO Scope] = CAST(claim_mto_scope AS nvarchar(255))
		,[Claim Resolution Statistic (Days/Count)] = CASE
														WHEN COALESCE(CAST((
																(DATEDIFF(dd, claim_mto_scope, claim_year) + 1)
																	-(DATEDIFF(wk, claim_mto_scope, claim_year) * 2)
																	-(CASE WHEN DATENAME(dw, claim_mto_scope) = 'Sunday' THEN 1 ELSE 0 END)
																	-(CASE WHEN DATENAME(dw, claim_year) = 'Saturday' THEN 1 ELSE 0 END)
																) AS nvarchar(255)),0) = 0 THEN NULL 
														ELSE	CAST((
																(DATEDIFF(dd, claim_mto_scope, claim_year) + 1)
																	-(DATEDIFF(wk, claim_mto_scope, claim_year) * 2)
																	-(CASE WHEN DATENAME(dw, claim_mto_scope) = 'Sunday' THEN 1 ELSE 0 END)
																	-(CASE WHEN DATENAME(dw, claim_year) = 'Saturday' THEN 1 ELSE 0 END)
																) / SUM(requests_resolved)
															AS nvarchar(255)) END							 

	FROM {{ref('dm__to_kpi_t_fact_claimexecutivesummary')}} es 
		LEFT JOIN CLAIM_DATES cd ON cd.business_area = es.business_area
	WHERE 1=1
		AND es.[year] = 'Resolved Total'
	GROUP BY
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,project_id
		,claim_year
		,claim_mto_scope
	),

UNPIVOTTED_DATA AS (
	SELECT
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,project_id
		,[description]
		,[value]
	FROM RAW_DATA

	UNPIVOT

	([description] FOR [value] IN ([Inclusion of Claim Processes in MTO Scope] 
			,claim_year
			,[Total Number of Concluded Claims]
			,[Claim Resolution Statistic (Days/Count)])) AS ABC
)

SELECT
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,project_id
	,[description] = CASE
						 WHEN LEN([description]) > 15 THEN CAST(CAST([description] AS DATE) AS NVARCHAR(255))
					 ELSE [description] END
	,[value]
FROM UNPIVOTTED_DATA


