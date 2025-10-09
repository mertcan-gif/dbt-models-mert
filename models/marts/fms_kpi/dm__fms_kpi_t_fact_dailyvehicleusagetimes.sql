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

,vehicle_usage_time_raw_data AS (
	SELECT
		Cihaz_x0020_No AS node
		,REPLACE(Plaka, ' ', '') AS license_plate
		,CAST(StartDate AS datetime) AS start_date
	    ,CAST(EndDate AS datetime) AS end_date
		,COALESCE(SUM(TRY_CAST(Kontak_x0020_Açık_x0020_Kalma_x0020_Süresi_x0020_sa AS INT)),0) AS total_hours
		,COALESCE(SUM(TRY_CAST(Kontak_x0020_Açık_x0020_Kalma_x0020_Süresi_x0020_dak AS INT)),0) AS total_minutes
		,COALESCE(SUM(TRY_CAST(Kontak_x0020_Açık_x0020_Kalma_x0020_Süresi_x0020_sn AS INT)),0) AS total_seconds
		,CAST(db_upload_timestamp AS date) AS reporting_date
	FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_fact_ignitiondurationreport') }}  idr
	WHERE Plaka NOT IN ('None')
		AND Cihaz_x0020_No <> 'nan'
	GROUP BY 
		Cihaz_x0020_No, 
		Plaka, 
		CAST(StartDate AS datetime), 
		CAST(EndDate AS datetime),
		CAST(db_upload_timestamp AS date)
)
,driver_info AS (
	SELECT 
		 REPLACE (EQUNR, ' ', '') AS license_plate
		,CAST(db_upload_timestamp AS date) AS reporting_date
		,CONCAT(ZZITO_ARAC_SURUCU_AD, ' ', ZZITO_ARAC_SURUCU_SOYAD) AS driver
		,GSBER
		,BUKRS
		,ROW_NUMBER() OVER (PARTITION BY EQUNR, CAST(db_upload_timestamp as date) ORDER BY CAST(db_upload_timestamp as date)) AS rn
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vehicles') }}
)
,data_manipulation AS (
	SELECT 
		vt.node
		,d.driver
		,d.license_plate
		,d.BUKRS
		,d.GSBER
		,vt.start_date
		,vt.end_date
		,vt.total_seconds
		,vt.total_minutes
		,vt.total_hours
	FROM vehicle_usage_time_raw_data vt
	LEFT JOIN driver_info d on  vt.license_plate = d.license_plate
							AND CAST(vt.end_date AS date) = d.reporting_date
	WHERE rn = 1
)
,vehicle_usage_time AS (
	SELECT
		node,
		driver,
		license_plate,
		start_date,
		end_date,
		BUKRS,
		GSBER,
		total_seconds,
		total_minutes,
		total_hours,
		last_hour_of_day_for_vehicle = ROW_NUMBER() OVER (PARTITION BY license_plate, node, CAST(end_date as date) ORDER BY end_date desc)
	FROM data_manipulation
)

SELECT 
	[rls_region] = (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT),

	[rls_group] = CONCAT(
							(SELECT TOP 1 [group] FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT)
							,'_',
							(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT) 
						),
	[rls_company] = CONCAT(
							BUKRS,'_',
							(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT)
						),
	[rls_businessarea] = CONCAT(
								GSBER,'_',
								(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT)
						),
	company = BUKRS,
	businessarea = GSBER,
	node,
	driver,
	license_plate,
	start_date,
	end_date,
	total_hours = total_hours + (total_minutes + total_seconds / 60) / 60,
    total_minutes = (total_minutes + total_seconds / 60) % 60,
    total_second = total_seconds % 60
FROM vehicle_usage_time
WHERE last_hour_of_day_for_vehicle = 1
