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

,license_plate_dimension AS (
	SELECT DISTINCT
		EQUNR,
		BUKRS,
		GSBER
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vehicles') }}
)

,node_dimension AS (
	SELECT DISTINCT 
		 [Node]
		,lpd.EQUNR as vehicle_license_plate
		,CAST(reporting_date AS date) AS reporting_date
		,BUKRS
		,GSBER
	FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_dim_vehiclenodes') }} vn
	RIGHT JOIN license_plate_dimension lpd on vn.LicensePlate=lpd.EQUNR
	WHERE LicensePlate IS NOT NULL
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
	,company = vn.BUKRS
	,businessarea = vn.GSBER
    ,vn.Node as vehicle_node
    ,vehicle_license_plate
    ,vs.[reporting_date]
    ,CAST(CONVERT(DATETIMEOFFSET, [GMT_x0020_Tarih_x002F_Saat] , 126) AS datetime) AS last_transaction_date
    ,[Enlem] as latitude
    ,[Boylam] as longitude
    ,[Hız] as velocity
    ,[İl] as city
    ,[İlçe] as district
FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_fact_vehiclestatus') }} vs
RIGHT JOIN node_dimension vn on vn.[Node] = vs.[Cihaz_x0020_No] 
								AND CAST(vs.reporting_date AS date) = vn.reporting_date