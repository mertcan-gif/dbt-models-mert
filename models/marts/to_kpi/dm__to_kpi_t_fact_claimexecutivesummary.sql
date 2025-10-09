{{
  config(
    materialized = 'table',tags = ['to_kpi','claimexecutivesummary']
    )
}}
WITH RESOLVED_RAW AS (
	SELECT
		business_area
		,[year] = CONCAT(CAST([year] AS nvarchar(4)),' - Completed')
		,requests_resolved = COUNT(*)
		,amount_requested = (SELECT SUM(COALESCE(claim_total_amount_try,0)) FROM {{ source('stg_to_kpi', 'raw__to_kpi_t_fact_claimdata') }} cd2 
								WHERE cd.[year] = cd2.[year] AND cd2.[status] = N'APPROVED'
								AND COALESCE(cd2.business_area, 'blank') = COALESCE(cd.business_area, 'blank'))
		,amount_agreed = (SELECT SUM(COALESCE(approved_total_amount_try,0)) FROM {{ source('stg_to_kpi', 'raw__to_kpi_t_fact_claimdata') }} cd2 
							WHERE cd.[year] = cd2.[year] AND cd2.[status] = N'APPROVED'
							AND COALESCE(cd2.business_area, 'blank') = COALESCE(cd.business_area, 'blank'))
		,order_rank = DENSE_RANK() OVER(ORDER BY [year])
	FROM {{ source('stg_to_kpi', 'raw__to_kpi_t_fact_claimdata') }} cd 
	WHERE 1=1
		AND [status] <> N'CALCULATED' 
		AND [status] <> N'ON-GOING'
	GROUP BY 
		business_area
		,[year]
),
RESOLVED AS (
	SELECT
		business_area
		,[year]
		,requests_resolved
		,amount_requested
		,amount_agreed
		,ratio = CASE WHEN (amount_requested = 0 OR amount_requested IS NULL) THEN 0 ELSE amount_agreed / amount_requested END
		,benefit = amount_requested - amount_agreed
		,order_rank
	FROM RESOLVED_RAW
	UNION ALL 
	SELECT DISTINCT
		business_area
		,[year] =   CONCAT(CAST(YEAR(GETDATE()) AS nvarchar(4)),' - Completed')
		,requests_resolved = 0
		,amount_requested = 0
		,amount_agreed = 0
		,ratio = 0
		,benefit = 0
		,order_rank = '6'
	FROM RESOLVED_RAW
),
FIRMALARA_GONDERILEN AS (
	SELECT
		business_area
		,[year] = CONCAT(CAST(YEAR(GETDATE()) AS nvarchar(4)),' - Sent to Subcontractors')
		,requests_resolved = COUNT(*)
		,amount_requested = SUM(claim_total_amount_try)
		,A = NULL
		,B = NULL
		,C = NULL
		,order_rank = '8'
	FROM {{ source('stg_to_kpi', 'raw__to_kpi_t_fact_claimdata') }} cd
	WHERE 1=1
		AND [status] = N'CALCULATED' 
	GROUP BY 
		business_area
),
CALISMASI_DEVAM_EDEN AS (
	SELECT
		business_area
		,[year] = CONCAT(CAST(YEAR(GETDATE()) AS nvarchar(4)),' - On-Going') 
		,requests_resolved = 
			COALESCE((SELECT COUNT(*) FROM {{ source('stg_to_kpi', 'raw__to_kpi_t_fact_claimdata') }} cd3 
						 WHERE COALESCE(cd3.business_area, 'blank') = COALESCE(cd.business_area, 'blank')
							AND cd3.[status] NOT LIKE N'%ON HOLD/HANDED OVER%'
							AND cd3.[status] <> N'APPROVED'),0)
			- COALESCE((SELECT requests_resolved FROM FIRMALARA_GONDERILEN fg WHERE COALESCE(fg.business_area, 'blank') = COALESCE(cd.business_area, 'blank')),0)
		,amount_requested = COALESCE((SELECT SUM(COALESCE(claim_total_amount_try,0)) FROM {{ source('stg_to_kpi', 'raw__to_kpi_t_fact_claimdata') }} cd2 WHERE [status] = 'ON-GOING' AND COALESCE(cd2.business_area, 'blank') = COALESCE(cd.business_area, 'blank')),0)
		,A = NULL
		,B = NULL
		,C = NULL
		,order_rank = '9'
	FROM {{ source('stg_to_kpi', 'raw__to_kpi_t_fact_claimdata') }} cd
	WHERE 1=1
		AND [status] NOT LIKE N'%ON HOLD/HANDED OVER%' 
		AND [status] <> N'APPROVED'
	GROUP BY 
		business_area
),
UNRESOLVED_UNION AS (
	SELECT
		business_area
		,[year] = 'Total'
		,[requests_resolved] = SUM(COALESCE(requests_resolved,0))
		,[amount_requested] = SUM(COALESCE(amount_requested,0))
		,[amount_agreed] = SUM(COALESCE(amount_agreed,0))
		,ratio = CASE WHEN SUM(COALESCE(amount_requested,0)) = 0 THEN 0 ELSE SUM(amount_agreed) / SUM(COALESCE(amount_requested,0)) END
		,[benefit] = SUM(COALESCE(benefit,0))
		,order_rank = '7'
	FROM RESOLVED
	GROUP BY 
		business_area
	UNION ALL
	SELECT * FROM FIRMALARA_GONDERILEN
	UNION ALL
	SELECT * FROM CALISMASI_DEVAM_EDEN
),
FINAL_DATA AS (
	SELECT * FROM RESOLVED
	UNION ALL
	SELECT * FROM UNRESOLVED_UNION
	UNION ALL
	SELECT 
		business_area
		,[year] = 'Grand Total' 
		,requests_resolved = SUM(requests_resolved)
		,amount_requested = SUM(amount_requested)
		,A = NULL
		,B = NULL
		,C = NULL
		,order_rank = '10'
	FROM UNRESOLVED_UNION
	GROUP BY 
		business_area
)

SELECT
	dim.region AS rls_region
	,CONCAT(COALESCE(dim.[group],''),'_',COALESCE(dim.region,'')) AS rls_group
	,CONCAT(COALESCE(dim.company,''),'_',COALESCE(dim.region,'')) AS rls_company 
	,CONCAT(COALESCE(dim.business_area,''),'_',COALESCE(dim.region,'')) AS rls_businessarea
	,dim.project_id
	,fd.*
FROM FINAL_DATA fd
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} dim ON dim.business_area = fd.business_area   
