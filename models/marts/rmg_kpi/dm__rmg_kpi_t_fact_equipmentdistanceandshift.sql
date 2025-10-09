{{
  config(
    materialized = 'table',tags = ['rmg_kpi']
    )
}}

WITH main_cte AS (
SELECT
	CASE 
		  WHEN CHARINDEX('-', plate, CHARINDEX('-', plate) + 1) > 0 
		  THEN LEFT(plate, CHARINDEX('-', plate, CHARINDEX('-', plate) + 1) - 1)
		  ELSE plate
	END AS equipment,
	muId AS equipment_id,
	brand,
	model
	plate,
	totalDistance AS distance,
	canbusKmBasedAvgFuelUsed AS avg_fuel_used_km,
	canFuelUsed AS fuel_used,
	idleTimeAsMinutes AS idle_time_as_minutes,
	CASE 
      WHEN idleTimeAsMinutes IS NOT NULL OR idleTimeAsMinutes <> 0 THEN (canFuelUsed / (idleTimeAsMinutes / 60))
      ELSE NULL
  END AS idle_fuel_per_hour,
	--CAST([date] AS datetime2) at time zone 'UTC' AS [date],
	workTimeAsMinutes AS work_time_as_minutes,
	stopTimeAsMinutes AS stop_time_as_minutes,
	moveTimeAsMinutes AS move_time_as_minutes,
	CAST(CAST([date] AS datetime2) AT TIME ZONE 'UTC' AT TIME ZONE 'Turkey Standard Time' AS datetime2) AS local_date_time,
	[dayOfWeek] AS day_of_week
FROM {{ source('stg_rmg_kpi', 'raw__rmg_kpi_t_fact_vehicle_reports') }}
)

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(eq.business_area, '_', rls_region),
	eq.company,
	eq.business_area,
	eq.equipment_group,
	eq.object_type,
	eq.registered_owner,
	main_cte.*
FROM main_cte
LEFT JOIN {{ ref('stg__rmg_kpi_t_dim_equipmentlist') }} eq ON main_cte.equipment = eq.equipment
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON eq.company = dim_comp.RobiKisaKod
