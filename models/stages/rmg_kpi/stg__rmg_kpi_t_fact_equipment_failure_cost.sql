{{
  config(
    materialized = 'table',tags = ['rmg_kpi']
    )
}}

SELECT 
	SUBSTRING(afih.qmnum, 5, LEN(afih.qmnum)) AS [notification],
	afih.equnr AS equipment,
	ktext AS _description,
	afih.aufnr AS order_number,
	auart AS order_type,
	CASE WHEN RIGHT(wrt01, 1) = '-' THEN -1 * CAST(LEFT(wrt01, LEN(wrt01) - 1) AS FLOAT) ELSE CAST(wrt01 AS FLOAT) END +
	CASE WHEN RIGHT(wrt02, 1) = '-' THEN -1 * CAST(LEFT(wrt02, LEN(wrt02) - 1) AS FLOAT) ELSE CAST(wrt02 AS FLOAT) END +
	CASE WHEN RIGHT(wrt03, 1) = '-' THEN -1 * CAST(LEFT(wrt03, LEN(wrt03) - 1) AS FLOAT) ELSE CAST(wrt03 AS FLOAT) END +
	CASE WHEN RIGHT(wrt04, 1) = '-' THEN -1 * CAST(LEFT(wrt04, LEN(wrt04) - 1) AS FLOAT) ELSE CAST(wrt04 AS FLOAT) END +
	CASE WHEN RIGHT(wrt05, 1) = '-' THEN -1 * CAST(LEFT(wrt05, LEN(wrt05) - 1) AS FLOAT) ELSE CAST(wrt05 AS FLOAT) END +
	CASE WHEN RIGHT(wrt06, 1) = '-' THEN -1 * CAST(LEFT(wrt06, LEN(wrt06) - 1) AS FLOAT) ELSE CAST(wrt06 AS FLOAT) END +
	CASE WHEN RIGHT(wrt07, 1) = '-' THEN -1 * CAST(LEFT(wrt07, LEN(wrt07) - 1) AS FLOAT) ELSE CAST(wrt07 AS FLOAT) END +
	CASE WHEN RIGHT(wrt08, 1) = '-' THEN -1 * CAST(LEFT(wrt08, LEN(wrt08) - 1) AS FLOAT) ELSE CAST(wrt08 AS FLOAT) END +
	CASE WHEN RIGHT(wrt09, 1) = '-' THEN -1 * CAST(LEFT(wrt09, LEN(wrt09) - 1) AS FLOAT) ELSE CAST(wrt09 AS FLOAT) END +
	CASE WHEN RIGHT(wrt10, 1) = '-' THEN -1 * CAST(LEFT(wrt10, LEN(wrt10) - 1) AS FLOAT) ELSE CAST(wrt10 AS FLOAT) END +
	CASE WHEN RIGHT(wrt11, 1) = '-' THEN -1 * CAST(LEFT(wrt11, LEN(wrt11) - 1) AS FLOAT) ELSE CAST(wrt11 AS FLOAT) END +
	CASE WHEN RIGHT(wrt12, 1) = '-' THEN -1 * CAST(LEFT(wrt12, LEN(wrt12) - 1) AS FLOAT) ELSE CAST(wrt12 AS FLOAT) END +
	CASE WHEN RIGHT(wrt13, 1) = '-' THEN -1 * CAST(LEFT(wrt13, LEN(wrt13) - 1) AS FLOAT) ELSE CAST(wrt13 AS FLOAT) END +
	CASE WHEN RIGHT(wrt14, 1) = '-' THEN -1 * CAST(LEFT(wrt14, LEN(wrt14) - 1) AS FLOAT) ELSE CAST(wrt14 AS FLOAT) END +
	CASE WHEN RIGHT(wrt15, 1) = '-' THEN -1 * CAST(LEFT(wrt15, LEN(wrt15) - 1) AS FLOAT) ELSE CAST(wrt15 AS FLOAT) END +
	CASE WHEN RIGHT(wrt16, 1) = '-' THEN -1 * CAST(LEFT(wrt16, LEN(wrt16) - 1) AS FLOAT) ELSE CAST(wrt16 AS FLOAT) END AS amount,
	cocur AS currency
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_afih') }} afih
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_aufk') }} aufk ON afih.aufnr = aufk.aufnr
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_pmco') }} pmco ON aufk.objnr = pmco.objnr
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eqkt') }} eqkt ON afih.equnr = eqkt.equnr
WHERE 1=1
AND pmco.cocur = 'TRY'
AND eqkt.spras = 'TR'