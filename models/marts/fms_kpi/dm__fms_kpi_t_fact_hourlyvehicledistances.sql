{{
  config(
    materialized = 'table',tags = ['fms_kpi']
    )
}}


{# Plaka olarak "demodamper" olarak bir değer dönüyor. Silinmesi gerekiyor mu netleştirilecek #}


WITH CompanyUnionMappingTable AS (
	SELECT 
		RobiKisaKod AS company,
		KyribaGrup AS [group],
		RegionCode AS region 
	FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }}
)

,hourly_vehicle_finish_km AS (
	SELECT 
		 [Cihaz_x0020_No] AS node
		,REPLACE([Plaka], ' ', '') AS license_plate
		,CASE 
			WHEN ISNUMERIC(REPLACE([Bitiş_x0020_Km], ',', '.')) = 1 
			THEN CAST(REPLACE([Bitiş_x0020_Km], ',', '.') AS FLOAT)
			ELSE NULL
		END AS finish_km
		,CAST(StartDate AS DATETIME) AS start_date
		,CAST(EndDate AS DATETIME) AS end_date
	FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_fact_fuelconsumptionreport') }}  
)
 
,driver_info AS (
	SELECT 
		 REPLACE (EQUNR, ' ', '') AS license_plate
		,CAST(db_upload_timestamp AS date) AS reporting_date
		,CONCAT(ZZITO_ARAC_SURUCU_AD, ' ', ZZITO_ARAC_SURUCU_SOYAD) AS driver
		,BUKRS
		,GSBER
		,ROW_NUMBER() OVER (PARTITION BY EQUNR, CAST(db_upload_timestamp as date) ORDER BY CAST(db_upload_timestamp as date)) AS rn
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vehicles') }}
)
 
,hourly_vehicle_usage AS (
	SELECT
		hvf.node
		,d.license_plate
		,d.driver
		,BUKRS
		,GSBER
		,hvf.finish_km
		,previous_finish_km = LAG(finish_km, 1) OVER (PARTITION BY hvf.license_plate, hvf.node ORDER BY start_date)
		,hvf.start_date
		,hvf.end_date
	FROM hourly_vehicle_finish_km hvf
	LEFT JOIN driver_info d on hvf.license_plate = d.license_plate
							AND CAST(hvf.end_date AS DATE) = d.reporting_date
	WHERE 1=1
		AND rn = 1
)
,hourly_distance AS ( 
SELECT 
	 [rls_region] = (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT)

	,[rls_group] = CONCAT(
							(SELECT TOP 1 [group] FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT)
							,'_',
							(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT) 
						)
	,[rls_company] = CONCAT(
							BUKRS,'_',
							(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT)
						)
	,[rls_businessarea] = CONCAT(
							GSBER,'_',
							(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT)
						)
	,company = BUKRS
	,businessarea = GSBER
	,node
	,license_plate
	,driver
	,finish_km
	,previous_finish_km
	,hourly_distance = CASE
							WHEN ROUND(finish_km - previous_finish_km,2) < 0 THEN 0
							ELSE ROUND(finish_km - previous_finish_km,2)
						END
	,start_date
	,end_date
FROM hourly_vehicle_usage hvu
WHERE 1=1
	AND ROUND(finish_km - previous_finish_km,2) >= 0
	)

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea,
	company,
	businessarea,
    node,
    license_plate,
    driver,
    MAX(finish_km) AS finish_km,
    SUM(hourly_distance) AS daily_distance,
	MAX(start_date) AS start_date,
    MAX(end_date) AS end_date
FROM 
    hourly_distance
GROUP BY 
    node,
    license_plate,
    driver,
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea,
	company,
	businessarea,
    CAST(end_date AS DATE),
	CAST(start_date AS DATE)

		