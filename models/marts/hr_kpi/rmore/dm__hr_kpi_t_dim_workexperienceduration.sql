{{
  config(
    materialized = 'table',tags = ['hr_kpi']
  )
}}

WITH hr_kpi AS ( 
    SELECT 
        [user_id]
        ,TRY_CAST([start_date] AS DATE) AS start_date
        ,CASE 
            WHEN [end_date] LIKE '%9999%' THEN CAST(GETDATE() AS DATE)
            WHEN [end_date] LIKE '%1753%' THEN NULL
            WHEN [end_date] LIKE '%0002%' THEN NULL
            WHEN [end_date] LIKE '%0001%' THEN NULL
            ELSE TRY_CAST([end_date] AS DATE)
        END AS [end_date]
       ,DATEDIFF(
            day
            ,TRY_CAST([start_date] AS DATE)
            ,CASE 
                WHEN [end_date] LIKE '%9999%' THEN CAST(GETDATE() AS DATE)
                WHEN [end_date] LIKE '%1753%' THEN NULL
                WHEN [end_date] LIKE '%0002%' THEN NULL
                WHEN [end_date] LIKE '%0001%' THEN NULL
                ELSE TRY_CAST([end_date] AS DATE)
            END
        ) AS duration_day
        ,'RONESANS' AS experience_type
    FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_insideworkexperience') }}
    WHERE [user_id] LIKE '47%' OR [user_id] LIKE 'GLB%'

    UNION ALL

    SELECT 
        [user_id]
        ,TRY_CAST([start_date] AS DATE) AS start_date
        ,CASE 
            WHEN [end_date] LIKE '%9999%' THEN CAST(GETDATE() AS DATE)
            WHEN [end_date] LIKE '%1753%' THEN NULL
            WHEN [end_date] LIKE '%0002%' THEN NULL
            WHEN [end_date] LIKE '%0001%' THEN NULL
            ELSE TRY_CAST([end_date] AS DATE)
        END AS [end_date]
        ,DATEDIFF(
             day 
            ,TRY_CAST([start_date] AS DATE)
            ,CASE 
                WHEN [end_date] LIKE '%9999%' THEN CAST(GETDATE() AS DATE)
                WHEN [end_date] LIKE '%1753%' THEN NULL
                WHEN [end_date] LIKE '%0002%' THEN NULL
                WHEN [end_date] LIKE '%0001%' THEN NULL
                ELSE TRY_CAST([end_date] AS DATE)
            END
        ) AS duration_day
        ,'NON RONESANS' AS experience_type
    FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_outsideworkexperience') }}
    WHERE [user_id] LIKE '47%' OR [user_id] LIKE 'GLB%'
)

SELECT
    	rls_region = rls.[rls_region]
	    ,rls_group = CONCAT(COALESCE([rls_group],''),'_',COALESCE([rls_region],''))
	    ,rls_company = CONCAT(COALESCE([rls_company],''),'_',COALESCE([rls_region],''))
	    ,rls_businessarea = CONCAT(COALESCE([rls_businessarea],''),'_',COALESCE([rls_region],''))
        ,rls_key=CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
    ,hr.[user_id]
	,CONCAT(emp.[name], ' ', emp.[surname]) AS full_name
    ,SUM(CASE WHEN hr.experience_type = 'RONESANS' THEN hr.duration_day ELSE 0 END) AS total_ronesans_days
    ,SUM(CASE WHEN hr.experience_type = 'NON RONESANS' THEN hr.duration_day ELSE 0 END) AS total_non_ronesans_days
FROM hr_kpi hr
LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }} emp
    ON emp.[user_id] = hr.[user_id]
LEFT JOIN {{ ref('stg__hr_kpi_t_dim_sf_rls') }} as rls 
    ON rls.[user_id] = hr.[user_id]
GROUP BY hr.[user_id], emp.[name], emp.[surname], 
         rls.[rls_region], rls.[rls_group], rls.[rls_company], rls.[rls_businessarea]

