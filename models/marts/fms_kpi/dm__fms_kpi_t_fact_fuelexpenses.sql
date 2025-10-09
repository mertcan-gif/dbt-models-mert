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

,all_fuel_data AS (
	SELECT
	  [fleet_name]
	  ,[license_plate]
	  ,[station_name]
	  ,[sales_date]
	  ,[unit_price]
	  ,[volume]
	  ,[product_name]
	  ,[total_amount]
	  ,[city]
	  ,[fuel_station_company] = 'shell'
	FROM {{ ref('stg__fms_kpi_t_fact_shellfuelexpenses') }}
	UNION ALL
	SELECT 
	  [fleet_name]
	  ,[license_plate]
	  ,[station_name]
	  ,[sales_date]
	  ,[unit_price]
	  ,[volume]
	  ,[product_name]
	  ,[total_amount]
	  ,[city]
	  ,[fuel_station_company] = 'opet'
	FROM {{ ref('stg__fms_kpi_t_fact_opetfuelexpenses') }}
	)

SELECT 
	/*
		Vehicles tablosuna yakıt alındıktan sonra gelen araçların rls kolonlarının NULL gelmemesi için vd ile joinleme yapılmıştır.
		Bu sayede o aracın vehicles tablosuna en son geldiği kısımdaki rls kolonları eklenmiştir.
	*/
  COALESCE(v.[rls_region], vd.rls_region) AS rls_region
  ,COALESCE(v.[rls_group], vd.rls_group) AS rls_group
  ,COALESCE(v.[rls_company], vd.rls_company) AS rls_company
  ,COALESCE(v.[rls_businessarea], vd.rls_businessarea) AS rls_businessarea
  ,COALESCE(v.[vehicle_using_company], vd.vehicle_using_company) AS company
  ,COALESCE(v.[business_area], vd.business_area) AS business_area
  ,COALESCE(v.[supply_type], vd.supply_type) AS supply_type
	,fd.*
FROM all_fuel_data fd
LEFT JOIN {{ ref('dm__fms_kpi_t_dim_vehicles') }} v on fd.license_plate = v.license_plate 
															AND v.reporting_date = CAST(fd.sales_date as date)
LEFT JOIN (SELECT
            rls_region
            ,rls_group
            ,rls_company
            ,rls_businessarea
            ,license_plate
            ,vehicle_using_company
            ,business_area
            ,supply_type
          FROM {{ ref('dm__fms_kpi_t_dim_vehicles') }}
          WHERE reporting_date = (SELECT 
                                    MAX(reporting_date) 
                                  FROM {{ ref('dm__fms_kpi_t_dim_vehicles') }})) vd ON fd.license_plate = vd.license_plate