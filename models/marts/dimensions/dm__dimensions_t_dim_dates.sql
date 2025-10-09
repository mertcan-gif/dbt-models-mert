{{
  config(
    materialized = 'table',tags = ['rmore','dimensions','fi_kpi']
    )
}}

SELECT 
    CAST(DATEADD(dd, number, '2020-01-01') AS DATE) AS [date],
    FORMAT(DATEADD(dd, number, '2020-01-01'), 'yyyyMMdd') AS date_sap_format,
    FORMAT(DATEADD(dd, number, '2020-01-01'), 'yyyy-MM') AS year_month,
    YEAR(DATEADD(dd, number, '2020-01-01')) AS year,
    MONTH(DATEADD(dd, number, '2020-01-01')) AS month,
    DATENAME(MONTH, DATEADD(dd, number, '2020-01-01')) AS month_description,
    DAY(DATEADD(dd, number, '2020-01-01')) AS day,
    DATEPART(WEEK, DATEADD(dd, number, '2020-01-01')) AS week,
    CASE 
        WHEN DATEPART(WEEKDAY, DATEADD(dd, number, '2020-01-01')) = 1 THEN 7
        ELSE DATEPART(WEEKDAY, DATEADD(dd, number, '2020-01-01')) - 1 
    END AS day_of_week,
	  DATENAME(WEEKDAY, DATEADD(dd, number, '2020-01-01')) AS day_of_week_description,
    'Q' + CAST(DATEPART(QUARTER, DATEADD(dd, number, '2020-01-01')) AS VARCHAR(1)) AS quarter,
    FORMAT(DATEADD(dd, number, '2020-01-01'), 'yyyy') + '-Q' + CAST(DATEPART(QUARTER, DATEADD(dd, number, '2020-01-01')) AS VARCHAR(1)) AS year_quarter
FROM 
    master..spt_values
WHERE 
    type = 'P' 
    AND DATEADD(dd, number, '2020-01-01') <= '2099-12-31';