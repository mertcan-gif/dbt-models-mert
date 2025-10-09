{{
  config(
    materialized = 'view', tags = ['hr_kpi']
    )
}}

WITH max_egitim AS (
    SELECT 
        [person_id]
        ,CASE
            WHEN egitim_seviyesi = 'Doctorate' THEN N'Doktora'
            WHEN egitim_seviyesi = 'Master degree' THEN N'Yüksek Lisans'
            WHEN egitim_seviyesi = 'Bachelor degree'  THEN N'Lisans'
            WHEN egitim_seviyesi = 'Associate degree' THEN N'Ön Lisans'
            WHEN egitim_seviyesi = 'High school'  THEN N'Lise'
            WHEN egitim_seviyesi = 'Primary education'  THEN N'Ilkögretim'
            WHEN egitim_seviyesi = 'Training seminar' THEN N'Egitim Semineri'
            ELSE NULL
        END [egitim_seviyesi_tr]
        , [egitim_seviyesi_en] = egitim_seviyesi
    FROM
    (SELECT *,
            ROW_NUMBER() OVER (PARTITION BY [person_id]
                                ORDER BY egitim_enum ASC) AS rn
    FROM
        (SELECT [kisi_taniticisi] AS [person_id],
                [egitim_seviyesi],
                CASE
                    WHEN [egitim_seviyesi] = 'Doctorate' THEN 1
                    WHEN [egitim_seviyesi] = 'Master degree' THEN 2
                    WHEN [egitim_seviyesi] = 'Bachelor degree' THEN 3
                    WHEN [egitim_seviyesi] = 'Associate degree' THEN 4
                    WHEN [egitim_seviyesi] = 'High school' THEN 5
                    WHEN [egitim_seviyesi] = 'Primary education' THEN 6
                    WHEN [egitim_seviyesi] = 'Training seminar' THEN 7
                    ELSE 8
                END AS egitim_enum
        FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_educationreport') }}) raw_data) raw_data2
    WHERE raw_data2.rn = 1

)

SELECT *
FROM max_egitim