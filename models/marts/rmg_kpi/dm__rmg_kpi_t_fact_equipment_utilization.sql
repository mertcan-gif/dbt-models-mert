{{
  config(
    materialized = 'table',tags = ['rmg_kpi']
    )
}}

WITH daily_summary AS (
SELECT 
	CASE 
		WHEN CHARINDEX('-', plate, CHARINDEX('-', plate) + 1) > 0 
		THEN LEFT(plate, CHARINDEX('-', plate, CHARINDEX('-', plate) + 1) - 1)
		ELSE plate
	END AS equipment,
	CAST([day] AS date) AS [date],
	DATEADD(MILLISECOND, workStartTime, CAST('00:00:00' as time)) as start_time,
	DATEADD(MILLISECOND, workEndTime, CAST('00:00:00' as time)) as end_time,
	--run_time_minutes = (cast(totalRunTime as float)/ 60000),
	--idling_time_minutes = (cast(totalIdleTime as float) / 60000),
	--downtime_minutes = (cast(totalStopTime as float) / 60000),
	run_time_hours = (cast(totalRunTime as float)/ 3600000),
	idling_time_hours = (cast(totalIdleTime as float) / 3600000),
	downtime_hours = (cast(totalStopTime as float) / 3600000),
	totalDailyDistance AS daily_distance_km
FROM {{ source('stg_rmg_kpi', 'raw__rmg_kpi_t_fact_dailysummary') }}
)

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(eq.business_area, '_', comp.rls_region),
	eq.company,
	eq.business_area,
	eq.equipment_group,
	eq.object_type,
	ds.*,
	ut.utilization_calculation_unit,
	ut.norm_limit,
	ut.price,
    CASE
        WHEN utilization_calculation_unit = 'hour' THEN (ds.run_time_hours / NULLIF(norm_limit, 0))
        WHEN utilization_calculation_unit = 'distance' THEN (ds.daily_distance_km / NULLIF(norm_limit, 0))
    END AS utilization_percentage
FROM daily_summary ds
LEFT JOIN {{ source('stg_rmg_kpi', 'raw__rmg_kpi_t_dim_utilization') }} ut ON ds.equipment = ut.equipment
LEFT JOIN {{ ref('stg__rmg_kpi_t_dim_equipmentlist') }} eq ON ds.equipment = eq.equipment
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} comp ON eq.company = comp.RobiKisaKod