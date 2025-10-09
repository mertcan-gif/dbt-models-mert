{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
WITH unpivoted_data AS (
SELECT 
    [office],
    [floor],
    unpivoted_area_type AS area_type,
    unpivoted_area_size AS area_size
FROM (
    SELECT 
        [office],
        [floor],
        CAST([office_m2] AS nvarchar) AS office_m2,
        CAST([parking_m2] AS nvarchar) AS parking_m2,
        CAST([cafetaria_m2] AS nvarchar) AS cafetaria_m2,
        CAST([conferencehall_m2] AS nvarchar) AS conferencehall_m2,
        CAST([corridor_m2] AS nvarchar) AS corridor_m2,
        CAST([stairs_elevator_m2] AS nvarchar) AS stairs_elevator_m2,
        CAST([wetarea_kitchen_m2] AS nvarchar) AS wetarea_kitchen_m2,
        CAST([technical_volume] AS nvarchar) AS technical_volume,
        CAST([balcony_terrace_m2] AS nvarchar) AS balcony_terrace_m2
    FROM {{ source('stg_sharepoint', 'raw__rmh_kpi_t_dim_officeareas') }}
) AS source_table
UNPIVOT (
    unpivoted_area_size FOR unpivoted_area_type IN (
        [office_m2], 
        [parking_m2], 
        [cafetaria_m2], 
        [conferencehall_m2], 
        [corridor_m2], 
        [stairs_elevator_m2], 
        [wetarea_kitchen_m2], 
        [technical_volume], 
        [balcony_terrace_m2]
    )
) AS unpivoted_table 
	)

SELECT 
	rls_region = cm.RegionCode
	,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
	,rls_company = cm.RobiKisaKod + '_' + cm.RegionCode
	,rls_businessarea = '_' + cm.RegionCode
	,company = cm.RobiKisaKod
	,
    /*
        Piazza ofisin masrafları kat bazlı atıldığı için office isimlerini bu şekilde güncellenmesi gerekmektedir.
    */
    CASE   
        WHEN office = 'PIAZZA' THEN CONCAT(office, '-', floor)
        ELSE office
    END AS office
	,floor
	,area_type
	,TRY_CAST(area_size as float) as area_size
FROM unpivoted_data
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON 'RMH' = cm.RobiKisaKod

