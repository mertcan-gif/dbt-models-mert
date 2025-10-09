{{
  config(
    materialized = 'table',tags = ['fi_kpi','dimensions']
    )
}}


WITH Dates as
	(
		SELECT 
			CAST(DATEADD(dd, number, '2022-01-01') AS DATE) Date
		FROM 
			master..spt_values m1
		WHERE 
			type = 'P' 
		AND DATEADD(dd, number, '2022-01-01') <= GETDATE()
	),

	try_cte_raw AS
	(
		SELECT CONVERT(DATE, LTRIM(RTRIM(gdatu)), 104) AS TARIH, FCURR , CASE WHEN CAST(ukurs AS FLOAT) < 0 THEN - 1 * CAST(ukurs AS FLOAT) ELSE CAST(ukurs AS FLOAT) END AS [try_value]
		FROM {{ ref('vw__s4hana_v_sap_ug_tcurr') }} AS T WITH (NOLOCK)
		WHERE 1=1
			AND (TCURR = 'TRY') 
			AND (KURST = 'M') AND (RIGHT(gdatu, 4) >= 2018)
	),

	DATE_CURR_TABLE AS (
		SELECT dc.FCURR, dd.Date
		FROM (SELECT DISTINCT FCURR FROM try_cte_raw) dc
		CROSS APPLY (
			SELECT Dates.Date
			FROM Dates
		) AS dd
	),

	CROSS_APPLIED_TABLE AS (
		SELECT 
			dct.Date
			,dct.FCURR
			,ac.try_value
		FROM DATE_CURR_TABLE dct
			LEFT JOIN try_cte_raw ac ON ac.TARIH = dct.Date
								AND  ac.FCURR = dct.FCURR
	),

	/**
	Bu CTE'de eksik olan günlerdeki kur değerleri en son günün kur değeri ile doldurulmuştur
	**/
	try_cte AS (
		SELECT
		  T.Date AS TARIH,
		  T.FCURR,
		  ISNULL(T.try_value, 
			(SELECT TOP 1 try_value 
				FROM CROSS_APPLIED_TABLE AS T1 
				WHERE 1=1
					AND Date < T.Date 
					AND try_value IS NOT NULL  
					AND T1.FCURR = T.FCURR
				ORDER BY Date DESC)) AS try_value
		FROM
		  CROSS_APPLIED_TABLE AS T
	),
	-- TRY_CTE END
	-- TRY_VALUES_ONLY START
	usd_curr AS
	(
		SELECT CONVERT(DATE, LTRIM(RTRIM(gdatu)), 104) AS TARIH, FCURR, 1/CAST(ukurs AS FLOAT) usd_value
		FROM     {{ ref('vw__s4hana_v_sap_ug_tcurr') }} AS T WITH (NOLOCK)
		WHERE 1=1
			AND (TCURR = 'TRY') AND (FCURR = 'USD')	AND (KURST = 'M') AND (RIGHT(gdatu, 4) >= 2018)
	), 
	eur_curr AS
	(
		SELECT CONVERT(DATE, LTRIM(RTRIM(gdatu)), 104) AS TARIH, FCURR, 1/CAST(ukurs AS FLOAT) eur_value
		FROM     {{ ref('vw__s4hana_v_sap_ug_tcurr') }} AS T WITH (NOLOCK)
		WHERE 1=1
			AND (TCURR = 'TRY') 
			AND (FCURR = 'EUR')
			AND (KURST = 'M') AND (RIGHT(gdatu, 4) >= 2018)
	),
	eur_curr_other AS (
		SELECT DISTINCT
				CONVERT(DATE, LTRIM(RTRIM(gdatu)), 104) AS TARIH1
				,FCURR
				,TCURR
				,1/CAST(ukurs AS FLOAT) as eur_value_other
		FROM {{ ref('vw__s4hana_v_sap_ug_tcurr') }} 
		WHERE 1=1
			AND TCURR  IN ('IQD','TZS','LYD')
			AND FCURR=  'EUR'	
			AND (KURST = 'M') AND (RIGHT(gdatu, 4) >= 2018)
	),
	mzm_to_usd AS (
		SELECT
			CONVERT(DATE, LTRIM(RTRIM(gdatu)), 104) AS TARIH1,
			FCURR,
			TCURR,
			1 / CAST(ukurs AS FLOAT) AS usd_value_other
		FROM {{ ref('vw__s4hana_v_sap_ug_tcurr') }}
		WHERE TCURR IN ('MZM', 'EUR', 'TRY')
			AND FCURR IN ('USD')
			AND KURST = 'M'
			AND RIGHT(gdatu, 4) >= 2018
	),


	try_values_only_raw AS (
		SELECT	
			dct.Date
			,'TRY' AS FCURR
			,1 AS try_value
			,usd_value
			,eur_value
		FROM Dates dct
			LEFT JOIN eur_curr ON eur_curr.TARIH = dct.Date
			LEFT JOIN usd_curr ON usd_curr.TARIH = dct.Date
	),

 

	/**
	Bu CTE'de eksik olan günlerdeki kur değerleri en son günün kur değeri ile doldurulmuştur
	**/
	try_values_only AS (
		SELECT
		  T.Date AS TARIH,
		  T.FCURR,
		  1 AS try_value,
		  ISNULL(T.usd_value, 
			(SELECT TOP 1 usd_value 
				FROM try_values_only_raw AS T1 
				WHERE 1=1
					AND Date < T.Date 
					AND usd_value IS NOT NULL  
				ORDER BY Date DESC)) AS usd_value,
		  ISNULL(T.eur_value, 
			(SELECT TOP 1 eur_value 
				FROM try_values_only_raw AS T1 
				WHERE 1=1
					AND Date < T.Date 
					AND eur_value IS NOT NULL  
				ORDER BY Date DESC)) AS eur_value
		FROM
		  try_values_only_raw AS T
	),
	-- TRY_VALUES_ONLY END

	SUMMARY AS (
	SELECT 
		date_string = CAST(CAST(try_cte.TARIH AS NVARCHAR(255)) AS DATE),
		currency = try_cte.FCURR ,
		try_cte.try_value,
		usd_value = try_value/(SELECT TOP 1 try_value FROM try_cte as t2 WHERE t2.FCURR = 'USD' AND try_cte.TARIH = t2.TARIH),
		eur_value = try_value/(SELECT TOP 1 try_value FROM try_cte as t2 WHERE t2.FCURR = 'EUR' AND try_cte.TARIH = t2.TARIH)


	FROM try_cte
	UNION ALL
	SELECT *

	FROM try_values_only
	UNION ALL
	SELECT 
		TARIH1 as 'date_string',
		TCURR as 'currency',
		NULL as 'try_value',
		NULL as 'usd_value',
		eur_value_other as 'eur_value'
		FROM  eur_curr_other
	UNION ALL
	SELECT
		T1.TARIH1 as 'date_string',
		T1.TCURR as 'currency',
		(T1.usd_value_other)/(SELECT T4.usd_value_other from mzm_to_usd T4 where T4.TARIH1 = T1.TARIH1 AND T4.TCURR = 'TRY' AND T4.FCURR = 'USD' ) AS 'try_value',
		(T1.usd_value_other) AS 'usd_value',
		T1.usd_value_other / T2.usd_value_other AS 'eur_value'
	FROM mzm_to_usd T1
	CROSS JOIN mzm_to_usd T2
	WHERE T1.TARIH1 = T2.TARIH1
		AND T1.TCURR = 'MZM'
		AND T2.TCURR = 'EUR'
	)

SELECT DISTINCT
	date_string AS date_value,
	FORMAT(CAST(CAST(date_string AS NVARCHAR(255)) AS DATE),'yyyyMMdd') date_value_string,
	FORMAT(CAST(CAST(date_string AS NVARCHAR(255)) AS DATE),'yyyyMMdd') date_string,
	currency,
	try_value,
	usd_value,
	eur_value
FROM SUMMARY
