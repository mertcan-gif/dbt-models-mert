{{
  config(
    materialized = 'table',tags = ['fms_kpi']
    )
}}

SELECT 
	v.rls_region
	,v.rls_group
	,v.rls_company
	,v.rls_businessarea
	,company = v.vehicle_owner_company
	,businessarea = v.business_area
	,d.date
	,v.license_plate
	,CASE 
		WHEN v.usage_type IN (N'Genel Kullanım - Operasyon', N'Genel Kullanım - Dış Seyahatler', N'Genel Kullanım - Makam') THEN 'Havuz'
		ELSE 'Tahsisli'
	END AS usage_type
	,dvt.total_hours
	,dvt.total_minutes
	,dvt.total_second
	,hvd.daily_distance
	,CASE
	 WHEN usage_type IN (N'Genel Kullanım - Operasyon', N'Genel Kullanım - Dış Seyahatler', N'Genel Kullanım - Makam') THEN 
		IIF(
			((dvt.total_hours * 3600 + dvt.total_minutes * 60 + dvt.total_second) / 14400.0) * 100 > 100 --havuz araçlar için 
			,100
			,((dvt.total_hours * 3600 + dvt.total_minutes * 60 + dvt.total_second) / 14400.0) * 100
				)
	ELSE
		IIF (
			(hvd.daily_distance / 30) * 100 > 100 --tahsisli araçlar için
			,100
			,(hvd.daily_distance / 30) * 100   
				)
	 END AS fleet_utilization
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
	SELECT
		license_plate
		,total_hours
		,total_minutes
		,total_second
		,end_date
	FROM {{ ref('dm__fms_kpi_t_fact_dailyvehicleusagetimes') }}
	WHERE DATEPART(HOUR, end_date) = 23) dvt on CAST(dvt.end_date as date) = d.date 
LEFT JOIN (
			SELECT
				license_plate
				,daily_distance
				,end_date
			FROM {{ ref('dm__fms_kpi_t_fact_hourlyvehicledistances') }}
			WHERE DATEPART(HOUR, end_date) = 23) hvd 
						on CAST(hvd.end_date as date) = d.date 
						AND hvd.license_plate = dvt.license_plate
LEFT JOIN (
	SELECT DISTINCT
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,vehicle_owner_company
	,business_area
	,[license_plate]
	,[usage_type]
	,[reporting_date]
FROM {{ ref('dm__fms_kpi_t_dim_vehicles') }}) v on v.license_plate = dvt.license_plate
														and v.reporting_date = d.date
WHERE 1=1
	AND DATEPART(WEEKDAY, d.date) NOT IN ('7', '1')
	AND (v.usage_type NOT IN ('Aile', 'Kurum', N'Koruma Aracı') or v.usage_type IS NULL)
	AND (d.date >= '2024-03-01' AND d.date < GETDATE()) 






