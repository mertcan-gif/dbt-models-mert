{{
  config(
    materialized = 'view', tags = ['sf_new_api']
    )
}}



WITH max_egitim AS (
    SELECT 
        [person_id]
        ,CASE
            WHEN level_of_education_name_en = 'Doctorate' THEN N'Doktora'
            WHEN level_of_education_name_en = 'Master degree' THEN N'Yüksek Lisans'
            WHEN level_of_education_name_en = 'Bachelor degree'  THEN N'Lisans'
            WHEN level_of_education_name_en = 'Associate degree' THEN N'Ön Lisans'
            WHEN level_of_education_name_en = 'High school'  THEN N'Lise'
            WHEN level_of_education_name_en = 'Primary education'  THEN N'Ilkögretim'
            WHEN level_of_education_name_en = 'Training seminar' THEN N'Egitim Semineri'
            ELSE NULL
        END [egitim_seviyesi_tr]
        ,[level_of_education_name_en] AS egitim_seviyesi_en
    FROM
    (SELECT *,
            ROW_NUMBER() OVER (PARTITION BY [person_id]
                                ORDER BY egitim_enum ASC) AS rn
    FROM
        (SELECT user_id AS [person_id],
                [level_of_education_name_en],
				[level_of_education_name_tr],
                CASE
                    WHEN [level_of_education_name_en] = 'Doctorate' THEN 1
                    WHEN [level_of_education_name_en] = 'Master degree' THEN 2
                    WHEN [level_of_education_name_en] = 'Bachelor degree' THEN 3
                    WHEN [level_of_education_name_en] = 'Associate degree' THEN 4
                    WHEN [level_of_education_name_en] = 'High school' THEN 5
                    WHEN [level_of_education_name_en] = 'Primary education' THEN 6
                    WHEN [level_of_education_name_en] = 'Training seminar' THEN 7
                    ELSE 8
                END AS egitim_enum
        FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employee_education') }}) raw_data) raw_data2
    WHERE raw_data2.rn = 1

)

SELECT *
FROM max_egitim