{{
  config(
    materialized = 'table',tags = ['hr_kpi']
  )
}}

WITH hr_kpi AS ( 
    SELECT 
        user_id
        ,CAST(start_date AS DATE) AS start_date
        ,CAST(end_date AS DATE) AS end_date
        ,title AS job_title
        ,'RONESANS' AS experience_type
    FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_insideworkexperience') }}
    WHERE user_id LIKE '47%'
          OR user_id LIKE 'GLB%'

    UNION ALL

    SELECT 
        user_id
        ,CAST(start_date AS DATE) AS start_date
        ,CAST(end_date AS DATE) AS end_date
        ,job_title
        ,'NON RONESANS' AS experience_type
    FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_outsideworkexperience') }}
    WHERE user_id LIKE '47%'
          OR user_id LIKE 'GLB%'
)

,employees_birth AS (
    SELECT 
        user_id
        ,CAST(date_of_birth AS DATE) AS date_of_birth
    FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }}
)
-- Tagging logic for various conditions

,tagged_data AS (
    SELECT 
        hr.user_id
        ,emp.date_of_birth
        ,hr.start_date
        ,hr.end_date
        ,hr.job_title
        ,hr.experience_type
        ,'Future Start' AS tag_reason
    FROM hr_kpi hr
    LEFT JOIN employees_birth emp ON hr.user_id = emp.user_id
    WHERE hr.start_date > GETDATE()

    UNION ALL

    SELECT 
        hr.user_id
        ,emp.date_of_birth
        ,hr.start_date
        ,hr.end_date
        ,hr.job_title
        ,hr.experience_type
        ,'End < Start' AS tag_reason
    FROM hr_kpi hr
    LEFT JOIN employees_birth emp ON hr.user_id = emp.user_id
    WHERE hr.end_date IS NOT NULL AND hr.end_date < hr.start_date

    UNION ALL

    SELECT 
        hr.user_id
        ,emp.date_of_birth
        ,hr.start_date
        ,hr.end_date
        ,hr.job_title
        ,hr.experience_type
        ,'Before Birth' AS tag_reason
    FROM hr_kpi hr
    LEFT JOIN employees_birth emp ON hr.user_id = emp.user_id
    WHERE emp.date_of_birth IS NOT NULL AND hr.start_date < emp.date_of_birth

    UNION ALL

    SELECT 
        hr.user_id
        ,emp.date_of_birth
        ,hr.start_date
        ,hr.end_date
        ,hr.job_title
        ,hr.experience_type
        ,'Unrealistic Date' AS tag_reason
    FROM hr_kpi hr
    LEFT JOIN employees_birth emp ON hr.user_id = emp.user_id
    WHERE (YEAR(hr.start_date) NOT BETWEEN 1753 AND 9999)
       OR (YEAR(hr.end_date) NOT BETWEEN 1753 AND 9999)
)

,overlap_check AS (
    SELECT 
        user_id
        ,start_date
        ,end_date
        ,job_title
        ,experience_type
        ,LEAD(start_date) OVER (PARTITION BY user_id ORDER BY start_date) AS next_start_date
        ,LEAD(end_date) OVER (PARTITION BY user_id ORDER BY start_date) AS next_end_date
    FROM hr_kpi
)

,tagged_overlap AS (
    SELECT 
        user_id
        ,NULL AS date_of_birth -- date_of_birth is not relevant for overlap tagging
        ,start_date
        ,end_date
        ,job_title
        ,experience_type
        ,'Overlap' AS tag_reason
    FROM overlap_check
    WHERE end_date IS NOT NULL 
      AND next_start_date IS NOT NULL 
      AND end_date > next_start_date
)

,finals AS (
    SELECT 
        user_id
        ,date_of_birth
        ,start_date
        ,end_date
        ,job_title
        ,experience_type
        ,tag_reason
    FROM tagged_data

    UNION ALL

    SELECT 
        user_id
        ,date_of_birth
        ,start_date
        ,end_date
        ,job_title
        ,experience_type
        ,tag_reason
    FROM tagged_overlap
)

SELECT 
		rls_region = rls.[rls_region]
		,rls_group = rls.[rls_group]
		,rls_company = rls.[rls_company]
		,rls_businessarea = rls.[rls_businessarea]
        ,rls_key=CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
        ,f.*
FROM finals as f
LEFT JOIN {{ ref('stg__hr_kpi_t_dim_sf_rls') }} as rls ON rls.[user_id] = f.[user_id]