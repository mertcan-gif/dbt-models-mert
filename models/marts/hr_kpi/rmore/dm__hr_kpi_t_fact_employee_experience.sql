{{
  config(
    materialized = 'table',tags = ['hr_kpi','rmore']
    )
}}

/* 
Date: 20250905
Creator: Adem Numan Kaya
Report Owner: Adem Numan Kaya
Explanation: Experience table is created for RMore AI. 
*/

WITH experience AS (
    SELECT 
        [user_id],
        [current_title] AS title,
        [job_title],
        CAST([start_date] AS DATE) AS experience_start_date,
        CASE 
            WHEN [end_date] like '9999-%' THEN CAST(GETDATE() AS DATE)
            ELSE CAST([end_date] AS DATE) 
        END AS experience_end_date,
        [job_description],
        [present_employer] AS company,
        NULL AS department,
        NULL AS working_location,
        'Non-Rönesans' AS experience_type
    FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_outsideworkexperience') }}

    UNION ALL

    SELECT 
        [user_id],
        [title],
        NULL AS job_title,
        CAST([start_date] AS DATE) AS experience_start_date,
        CASE 
            WHEN [end_date] like '9999-%' THEN CAST(GETDATE() AS DATE)
            ELSE CAST([end_date] AS DATE) 
        END AS experience_end_date,
        NULL AS job_description,
        [company],
        [department],
        [working_location],
        'Rönesans' AS experience_type
    FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_insideworkexperience') }}
)

SELECT 
    rls.rls_region
    ,rls.rls_group
    ,rls.rls_company
    ,rls.rls_businessarea
    ,experience.[user_id]
    ,experience.[title]
    ,experience.[job_title]
    ,experience.[experience_start_date]
    ,experience.[experience_end_date]
    ,experience.[job_description]
    ,experience.[company]
    ,experience.[department]
    ,experience.[working_location]
    ,experience.[experience_type]
    ,CAST(datediff(DAY, experience_start_date,experience_end_date) AS FLOAT) as experience_in_days
FROM experience
LEFT JOIN {{ ref('stg__hr_kpi_t_dim_sf_rls') }} as rls on rls.user_id=experience.user_id
where 1=1
    and CAST(datediff(DAY, experience_start_date,experience_end_date) AS FLOAT)>=0  -- experience must be positive in days
    and experience.experience_start_date <> CAST('1753-01-01' as date) -- this date is dummy date that SF assign if there is no start_date
