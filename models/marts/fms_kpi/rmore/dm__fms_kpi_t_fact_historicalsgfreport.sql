{{
  config(
    materialized = 'table', tags = ['fms_kpi']
  )
}}

WITH sgf_data AS (
    SELECT 
        sgf.per_alankod AS company_code
        ,sgf.durum COLLATE Latin1_General_CI_AS AS status
        ,sgf.seyahatsebebi COLLATE Latin1_General_CI_AS AS travel_reason
        ,sgf.sicil COLLATE Latin1_General_CI_AS AS sap_id
        ,CAST(sgf.bastarih AS DATE) AS start_date
        ,CAST(sgf.bitistarih AS DATE) AS end_date
        ,CAST(sgf.tarih AS DATE) AS creation_date
    FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zsgf_t_01') }} AS sgf
    WHERE sgf.durum IN (N'DEVAM EDIYOR', N'TAMAMLANDI')
      AND sgf.bastarih <> '0000-00-00'
),

emp_data AS (
    SELECT 
        fms.start_date
        ,fms.end_date
        ,DATEDIFF(DAY, CAST(fms.start_date AS DATE), CAST(fms.end_date AS DATE)) AS travel_duration_day
        ,DATEDIFF(DAY, fms.creation_date, fms.start_date) AS diff_creation_to_start
        ,fms.creation_date
        ,fms.company_code
        ,fms.sap_id
        ,emp.user_id
        ,fms.travel_reason
        ,emp.name
        ,emp.surname
        ,emp.payroll_company
        ,fms.status
    FROM sgf_data AS fms
    LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }} AS emp
        ON fms.sap_id = emp.sap_id COLLATE Latin1_General_CI_AS
),

finals AS (
    SELECT
        emp.start_date
        ,emp.end_date
        ,emp.creation_date
        ,emp.travel_duration_day
        ,emp.diff_creation_to_start
        ,emp.company_code
        ,emp.payroll_company
        ,emp.name
        ,emp.surname
        ,emp.sap_id
        ,emp.user_id
        ,emp.status
        ,emp.travel_reason
        ,is_travel_started_before_creation = case when emp.diff_creation_to_start<0 then 1 else 0 end 
    FROM emp_data AS emp
)

SELECT 
    CONCAT('_', dim.rls_region, '-', dim.rls_company, '-', dim.rls_group) AS rls_key
    ,dim.rls_region
    ,dim.rls_group
    ,dim.rls_company
    ,CONCAT('_', dim.rls_region) AS rls_businessarea
    ,travel_started_date = finals.start_date
    ,travel_ended_date = finals.end_date
    ,travel_created_date = finals.creation_date
    ,travel_duration_day = finals.travel_duration_day
    ,diff_creation_to_start = finals.diff_creation_to_start
    ,finals.company_code
    ,finals.payroll_company
    ,finals.name
    ,finals.surname
    ,finals.sap_id
    ,finals.user_id
    ,finals.status
    ,finals.travel_reason
    ,finals.is_travel_started_before_creation 
FROM finals
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} as dim ON dim.robikisakod = finals.company_code    
