{{
  config(
    materialized = 'table',tags = ['fms_kpi']
    )
}}

WITH CompanyUnionMappingTable AS (
	SELECT 
		RobiKisaKod AS company,
		KyribaGrup AS [group],
		RegionCode AS region 
	FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }}
)

,raw_data as(
	SELECT 
		 Cihaz_x0020_No AS node
		,REPLACE(Plaka, ' ', '') AS license_plate
		,COALESCE(CAST(NULLIF(Rölanti_x0020_Süresi_x0020_sa, 'nan') AS int),0) AS idling_hour
		,COALESCE(CAST(NULLIF(Rölanti_x0020_Süresi_x0020_dak, 'nan') AS int),0) AS idling_minute
		,COALESCE(CAST(NULLIF(Rölanti_x0020_Süresi_x0020_sn, 'nan') AS int),0) AS idling_second
		,CAST(StartDate AS datetime) AS start_date
		,CAST(EndDate AS datetime) AS end_date
	FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_fact_idlingdurationreport') }}
	WHERE Cihaz_x0020_No <> 'nan'
)

,driver_info AS (
	SELECT DISTINCT
		 REPLACE (EQUNR, ' ', '') AS license_plate
		,CAST(db_upload_timestamp AS date) AS reporting_date
		,CONCAT(ZZITO_ARAC_SURUCU_AD, ' ', ZZITO_ARAC_SURUCU_SOYAD) AS driver
		,BUKRS
		,GSBER
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vehicles') }}
)
,idling_times as(
	SELECT 
		 rd.node
		,d.license_plate
		,d.driver
		,BUKRS
		,GSBER
		,SUM(idling_hour) AS idling_hours
		,SUM(idling_minute) AS idling_minutes
		,SUM(idling_second) AS idling_seconds
		,rd.start_date
		,rd.end_date
		,last_hour_of_day_for_vehicle = ROW_NUMBER() OVER (PARTITION BY d.license_plate, node, CAST(end_date as date) ORDER BY end_date desc)
	FROM raw_data rd
	LEFT JOIN driver_info d on rd.license_plate = d.license_plate
							AND CAST(rd.end_date AS date) = d.reporting_date
	GROUP BY 
		 rd.node
		,d.license_plate
		,d.driver
		,rd.start_date
		,rd.end_date
		,BUKRS
		,GSBER
)
 
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
	,driver
	,license_plate
	,idling_hour = idling_hours + (idling_minutes + idling_seconds / 60) / 60
    ,idling_minute = (idling_minutes + idling_seconds / 60) % 60
    ,idling_second = idling_seconds % 60
	,start_date
	,end_date
FROM idling_times
WHERE 1=1
	AND last_hour_of_day_for_vehicle = 1
	AND license_plate IS NOT NULL