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

,vehicle_debits AS (
	SELECT 
		PERNR
		,INVNR
		,full_name
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vehicledebits') }} r
	LEFT JOIN (
				SELECT DISTINCT 
					full_name
					,sap_id
					FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
				) hr on hr.sap_id = r.PERNR
	WHERE 1=1
		AND  db_upload_timestamp = (
									SELECT 
										MAX(db_upload_timestamp) 
									FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vehicledebits') }}
									)
		AND ZZSTAT = 2
		AND ENDDA = '99991231' )

,speed_info AS (
SELECT DISTINCT
	 [Cihaz_x0020_No] AS node
	,REPLACE([Plaka], ' ', '') AS license_plate
	,ROUND(CAST([Hız_x0020_km_x002F_s] AS float),2) AS speed
	,Adres AS adress
	,CAST(CONVERT(DATETIMEOFFSET, [Tarih_x002F_Saat] , 126) AS datetime) AS last_transaction_date
FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_fact_speedreport') }}
)

,driver_info AS (
	SELECT 
		 REPLACE (EQUNR, ' ', '') AS license_plate
		,CAST(db_upload_timestamp AS date) AS reporting_date
		,CASE
			WHEN ZZITO_ARAC_SURUCU_AD = '' THEN vd.full_name
			ELSE CONCAT(ZZITO_ARAC_SURUCU_AD, ' ', ZZITO_ARAC_SURUCU_SOYAD)
		 END AS driver
		,ROW_NUMBER() OVER (PARTITION BY EQUNR, CAST(db_upload_timestamp as date) ORDER BY CAST(db_upload_timestamp as date)) AS rn
		,BUKRS
		,GSBER
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vehicles') }} v
	LEFT JOIN vehicle_debits vd on v.EQUNR = vd.INVNR
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
	,company = d.BUKRS
	,businessarea = d.GSBER
	,s.node
	,s.license_plate
	,d.driver
	,s.speed
	,s.adress
	,s.last_transaction_date
FROM speed_info s
LEFT JOIN driver_info d
		ON s.license_plate = d.license_plate 
		AND CAST(s.last_transaction_date AS date) = d.reporting_date
WHERE rn = 1

