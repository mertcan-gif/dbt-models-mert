{{
  config(
    materialized = 'table',tags = ['gyg_kpi']
    )
}}
WITH currency AS(
	SELECT 
		YEAR(CONVERT(DATE, gdatu, 104)) AS [year],
		MONTH(CONVERT(DATE, gdatu, 104)) AS [month],
		avg(CASE WHEN FCURR = 'EUR' THEN  CAST(UKURS AS MONEY) ELSE null END) AS [eur_to_try],
		avg(CASE WHEN FCURR = 'USD'  THEN  CAST(UKURS AS MONEY) ELSE null END) AS [usd_to_try]
	FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurr') }} AS T 
	WHERE 1=1
		AND (TCURR = 'TRY') 
		AND (KURST = 'BT') 
		--AND UKURS < 0
		AND YEAR(CONVERT(DATE, gdatu, 104)) > 2021
		GROUP BY
			YEAR(CONVERT(DATE, gdatu, 104)),
			MONTH(CONVERT(DATE, gdatu, 104))
	)
SELECT *
FROM currency