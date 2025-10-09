{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}

SELECT 
    *   
FROM {{ ref('dm__hr_kpi_t_fact_rmoreemployeeheadcount') }}
    WHERE 1=1
    AND MONTH([snapshot_date]) = 12
    AND DAY([snapshot_date]) = 31
AND {{ ref('dm__hr_kpi_t_fact_rmoreemployeeheadcount') }}."employee_status_en" = 'Active'
AND (
    {{ ref('dm__hr_kpi_t_fact_rmoreemployeeheadcount') }}."employee_type_en" = 'EYT'
OR {{ ref('dm__hr_kpi_t_fact_rmoreemployeeheadcount') }}."employee_type_en" = 'Normal'
    OR {{ ref('dm__hr_kpi_t_fact_rmoreemployeeheadcount') }}."employee_type_en" = 'Oryantasyon'
    OR {{ ref('dm__hr_kpi_t_fact_rmoreemployeeheadcount') }}."employee_type_en" = 'Pusula Kadro')
    --AND business_function IN (N'YÖNETİCİ',N'ŞEF',N'MÜDÜR',N'DİREKTÖR',N'KOORDİNATÖR',N'GENEL MÜDÜR YARDIMCISI',N'GENEL MÜDÜR YÖNETİM KURULU ÜYESİ')

