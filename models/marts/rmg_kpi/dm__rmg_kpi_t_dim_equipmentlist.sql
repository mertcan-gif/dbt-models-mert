{{
  config(
    materialized = 'incremental',
    unique_key = ['equipment', 'snapshot_date'],
    incremental_strategy = 'delete+insert',
    tags = ['rmg_kpi']
    )
}}

WITH locations AS (
SELECT 
	muId AS equipment_id,
	plate,
		CASE 
			WHEN CHARINDEX('-', plate, CHARINDEX('-', plate) + 1) > 0 
			THEN LEFT(plate, CHARINDEX('-', plate, CHARINDEX('-', plate) + 1) - 1)
			ELSE plate
		END AS equipment,
	city,
	town,
	longitude,
	latitude
FROM {{ source('stg_rmg_kpi', 'raw__rmg_kpi_t_dim_locations') }}
),

out_of_order AS (
	SELECT distinct equnr
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_maintenancenotification') }} mn
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_qmih') }} qmih ON mn.maintenancenotification = SUBSTRING(qmih.qmnum, 5, LEN(qmih.qmnum))
	WHERE 1=1
	AND maintenanceworkcenterplant = N'RMGM'
	AND notificationtype = 'Z1'
	AND isdeleted = ''
	AND iscompleted = ''
)

SELECT
    rls_region,
    rls_group,
    rls_company,
    rls_businessarea = CONCAT(business_area, '_', rls_region),
    eq.company,
    eq.business_area,
    eq.business_area_description,
    eq.registered_owner,
    eq.equipment_group,
    eq.object_type,
    eq.equipment,
	CASE 
		WHEN out_of_order.equnr is not null THEN 'out_of_order'
		WHEN out_of_order.equnr is null THEN 'running'
	END AS failure_status,
    eq.brand,
    eq.model,
    eq.plate,
	eq.model_year,
    age_of_equipment = CASE WHEN model_year <> ' ' THEN YEAR(GETDATE()) - model_year ELSE NULL END,
    purchase_price,
    CASE 
        WHEN tj.txt04 IN ('10', '20') THEN 'rented'
        WHEN tj.txt04 IN ('70', '80') THEN 'committed'
        WHEN tj.txt04 IN ('30', '40') THEN 'park'
        WHEN tj.txt04 IN ('50') THEN 'sold'
    END AS [status],
    locations.city,
    locations.longitude,
    locations.latitude,
    snapshot_date = CAST(GETDATE() AS DATE)
FROM {{ ref('stg__rmg_kpi_t_dim_equipmentlist') }} eq
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_jest') }} jest ON eq.object_number = jest.objnr
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tj30t') }} tj ON jest.stat = tj.estat
LEFT JOIN locations ON eq.equipment = locations.equipment
LEFT JOIN out_of_order ON eq.equipment = out_of_order.equnr
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} comp ON eq.company = comp.RobiKisaKod
WHERE 1=1
AND tj.stsma = N'ZEKIPMAN'
AND jest.INACT <> 'X'