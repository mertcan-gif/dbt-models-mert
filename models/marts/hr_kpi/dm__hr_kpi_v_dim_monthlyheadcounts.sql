{{
  config(
    materialized = 'view',tags = ['hr_kpi']
    )
}}


WITH eo_month_dates AS (
	SELECT Id=0 , CAST(GETDATE() AS DATE) AS end_of_month UNION ALL 
	SELECT Id=1 , DATEADD(MONTH,-1,EOMONTH(GETDATE())) UNION ALL 
	SELECT Id=2 , DATEADD(MONTH,-2,EOMONTH(GETDATE())) UNION ALL 
	SELECT Id=3 , DATEADD(MONTH,-3,EOMONTH(GETDATE())) UNION ALL 
	SELECT Id=4 , DATEADD(MONTH,-4,EOMONTH(GETDATE())) UNION ALL 
	SELECT Id=5 , DATEADD(MONTH,-5,EOMONTH(GETDATE())) UNION ALL 
	SELECT Id=6 , DATEADD(MONTH,-6,EOMONTH(GETDATE())) UNION ALL 
	SELECT Id=7 , DATEADD(MONTH,-7,EOMONTH(GETDATE())) UNION ALL 
	SELECT Id=8 , DATEADD(MONTH,-8,EOMONTH(GETDATE())) UNION ALL 
	SELECT Id=9 , DATEADD(MONTH,-9,EOMONTH(GETDATE()))
),
hr_headcount_monthly AS
(
	-- Güncel Tarih
		SELECT 
			(SELECT end_of_month FROM eo_month_dates Where id = 0) AS year_month
			,rls_region
			,rls_group
			,rls_company
			,rls_businessarea
			,COUNT(CASE WHEN collar_type = 'WHITE COLLAR' AND gender = 'F' THEN 1 END)  AS 'white_collar_female_headcount'
			,COUNT(CASE WHEN collar_type = 'WHITE COLLAR' AND gender = 'M' THEN 1 END) AS 'white_collar_male_headcount'
			--,COUNT(CASE WHEN collar_type = 'BLUE COLLAR' AND gender = 'F' THEN 1 END) AS 'blue_collar_female_headcount'
			--,COUNT(CASE WHEN collar_type = 'BLUE COLLAR' AND gender = 'M' THEN 1 END) AS 'blue_collar_male_headcount'
		FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
		WHERE 1=1 
			AND language = 'EN'	AND rls_region = 'TUR'
			AND [dwh_date_of_recruitment] <= (SELECT end_of_month FROM eo_month_dates Where id = 0)
			AND ([dwh_date_of_termination] >= (SELECT end_of_month FROM eo_month_dates Where id = 0)
				OR [dwh_date_of_termination] IS NULL)
			AND global_id <> ''
		GROUP BY 
			rls_region
			,rls_group
			,rls_company
			,rls_businessarea



	-- 1 Önceki Ay (Son günkü durum baz alınmıştır)
	UNION ALL
		SELECT 
			(SELECT end_of_month FROM eo_month_dates Where id = 1) AS year_month
			,rls_region
			,rls_group
			,rls_company
			,rls_businessarea
			,COUNT(CASE WHEN collar_type = 'WHITE COLLAR' AND gender = 'F' THEN 1 END) 
			,COUNT(CASE WHEN collar_type = 'WHITE COLLAR' AND gender = 'M' THEN 1 END) 
			--,COUNT(CASE WHEN collar_type = 'BLUE COLLAR' AND gender = 'F' THEN 1 END) 
			--,COUNT(CASE WHEN collar_type = 'BLUE COLLAR' AND gender = 'M' THEN 1 END)
		FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
		WHERE 1=1 
			AND language = 'EN'	AND rls_region = 'TUR'
			AND [dwh_date_of_recruitment] <= (SELECT end_of_month FROM eo_month_dates Where id = 1)
			AND ([dwh_date_of_termination] >= (SELECT end_of_month FROM eo_month_dates Where id = 1)
				OR [dwh_date_of_termination] IS NULL)
			AND global_id <> ''
		GROUP BY
			rls_region
			,rls_group
			,rls_company
			,rls_businessarea

	-- 2 Önceki Ay (Son günkü durum baz alınmıştır)
	UNION ALL
		SELECT
			(SELECT end_of_month FROM eo_month_dates Where id = 2) 
			,rls_region
			,rls_group
			,rls_company
			,rls_businessarea
			,COUNT(CASE WHEN collar_type = 'WHITE COLLAR' AND gender = 'F' THEN 1 END) 
			,COUNT(CASE WHEN collar_type = 'WHITE COLLAR' AND gender = 'M' THEN 1 END) 
			--,COUNT(CASE WHEN collar_type = 'BLUE COLLAR' AND gender = 'F' THEN 1 END) 
			--,COUNT(CASE WHEN collar_type = 'BLUE COLLAR' AND gender = 'M' THEN 1 END)
		FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
		WHERE 1=1 
			AND language = 'EN'	AND rls_region = 'TUR'
			AND [dwh_date_of_recruitment] <= (SELECT end_of_month FROM eo_month_dates Where id = 2)
			AND ([dwh_date_of_termination] >= (SELECT end_of_month FROM eo_month_dates Where id = 2)
				OR [dwh_date_of_termination] IS NULL)
			AND global_id <> ''
		GROUP BY 
			rls_region
			,rls_group
			,rls_company
			,rls_businessarea

	-- 3 Önceki Ay (Son günkü durum baz alınmıştır)
	UNION ALL
		SELECT
			(SELECT end_of_month FROM eo_month_dates Where id = 3) 
			,rls_region
			,rls_group
			,rls_company
			,rls_businessarea
			,COUNT(CASE WHEN collar_type = 'WHITE COLLAR' AND gender = 'F' THEN 1 END) 
			,COUNT(CASE WHEN collar_type = 'WHITE COLLAR' AND gender = 'M' THEN 1 END) 
			--,COUNT(CASE WHEN collar_type = 'BLUE COLLAR' AND gender = 'F' THEN 1 END) 
			--,COUNT(CASE WHEN collar_type = 'BLUE COLLAR' AND gender = 'M' THEN 1 END)
		FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
		WHERE 1=1 
			AND language = 'EN'	AND rls_region = 'TUR'
			AND [dwh_date_of_recruitment] <= (SELECT end_of_month FROM eo_month_dates Where id = 3)
			AND ([dwh_date_of_termination] >= (SELECT end_of_month FROM eo_month_dates Where id = 3)
				OR [dwh_date_of_termination] IS NULL)
			AND global_id <> ''
		GROUP BY 
			rls_region
			,rls_group
			,rls_company
			,rls_businessarea

	-- 4 Önceki Ay (Son günkü durum baz alınmıştır)
	UNION ALL
		SELECT
			(SELECT end_of_month FROM eo_month_dates Where id = 4) 
			,rls_region
			,rls_group
			,rls_company
			,rls_businessarea
			,COUNT(CASE WHEN collar_type = 'WHITE COLLAR' AND gender = 'F' THEN 1 END) 
			,COUNT(CASE WHEN collar_type = 'WHITE COLLAR' AND gender = 'M' THEN 1 END) 
			--,COUNT(CASE WHEN collar_type = 'BLUE COLLAR' AND gender = 'F' THEN 1 END) 
			--,COUNT(CASE WHEN collar_type = 'BLUE COLLAR' AND gender = 'M' THEN 1 END)
		FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
		WHERE 1=1 
			AND language = 'EN'	AND rls_region = 'TUR'
			AND [dwh_date_of_recruitment] <= (SELECT end_of_month FROM eo_month_dates Where id = 4)
			AND ([dwh_date_of_termination] >= (SELECT end_of_month FROM eo_month_dates Where id = 4)
				OR [dwh_date_of_termination] IS NULL)
			AND global_id <> ''
		GROUP BY 
			rls_region
			,rls_group
			,rls_company
			,rls_businessarea

	-- 5 Önceki Ay (Son günkü durum baz alınmıştır)
	UNION ALL
		SELECT
			(SELECT end_of_month FROM eo_month_dates Where id = 5) 
			,rls_region
			,rls_group
			,rls_company
			,rls_businessarea
			,COUNT(CASE WHEN collar_type = 'WHITE COLLAR' AND gender = 'F' THEN 1 END) 
			,COUNT(CASE WHEN collar_type = 'WHITE COLLAR' AND gender = 'M' THEN 1 END) 
			--,COUNT(CASE WHEN collar_type = 'BLUE COLLAR' AND gender = 'F' THEN 1 END) 
			--,COUNT(CASE WHEN collar_type = 'BLUE COLLAR' AND gender = 'M' THEN 1 END)
		FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
		WHERE 1=1 
			AND language = 'EN'	AND rls_region = 'TUR'
			AND [dwh_date_of_recruitment] <= (SELECT end_of_month FROM eo_month_dates Where id = 5)
			AND ([dwh_date_of_termination] >= (SELECT end_of_month FROM eo_month_dates Where id = 5)
				OR [dwh_date_of_termination] IS NULL)
			AND global_id <> ''
		GROUP BY 
			rls_region
			,rls_group
			,rls_company
			,rls_businessarea

	-- 6 Önceki Ay (Son günkü durum baz alınmıştır)
	UNION ALL
		SELECT
			(SELECT end_of_month FROM eo_month_dates Where id = 6) 
			,rls_region
			,rls_group
			,rls_company
			,rls_businessarea
			,COUNT(CASE WHEN collar_type = 'WHITE COLLAR' AND gender = 'F' THEN 1 END) 
			,COUNT(CASE WHEN collar_type = 'WHITE COLLAR' AND gender = 'M' THEN 1 END) 
			--,COUNT(CASE WHEN collar_type = 'BLUE COLLAR' AND gender = 'F' THEN 1 END) 
			--,COUNT(CASE WHEN collar_type = 'BLUE COLLAR' AND gender = 'M' THEN 1 END)
		FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
		WHERE 1=1 
			AND language = 'EN'	AND rls_region = 'TUR'
			AND [dwh_date_of_recruitment] <= (SELECT end_of_month FROM eo_month_dates Where id = 6)
			AND ([dwh_date_of_termination] >= (SELECT end_of_month FROM eo_month_dates Where id = 6)
				OR [dwh_date_of_termination] IS NULL)
			AND global_id <> ''
		GROUP BY 
			rls_region
			,rls_group
			,rls_company
			,rls_businessarea
	)
SELECT 
	[rls_region]
	,[rls_group]
	,[rls_company]
	,[rls_businessarea]
	,FORMAT([year_month],'yyyy-MM') AS [year_month]
	,[white_collar_female_headcount]
	,[white_collar_male_headcount]
	,blue_collar_female_headcount=NULL
	,blue_collar_male_headcount=NULL
FROM hr_headcount_monthly
