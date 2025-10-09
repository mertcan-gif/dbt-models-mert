{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}
WITH SICIL_RLS_MATCHING AS (
	SELECT *
	FROM (
		SELECT 
			rls_region
			,rls_group
			,rls_company
			,rls_businessarea
			,employee_id
            ,dwh_worksite												
			,ROW_NUMBER() OVER(PARTITION BY employee_id ORDER BY event_reason,age DESC) AS RN	
		FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
		WHERE language = 'EN'
	) RAW_DATA
	WHERE RAW_DATA.RN = 1
)

SELECT
     srm.rls_region
    ,srm.rls_group
    ,srm.rls_company
    ,srm.rls_businessarea
    ,UPPER(pt.[kisi_personel_numarasi])																					AS personnel_ID
    ,UPPER(pt.[global_id])																								AS global_id
    ,UPPER(pt.[sap_id])																									AS sap_id
    ,UPPER(pt.[adines_id])																								AS adines_id
    ,srm.dwh_worksite												                                                    AS business_area
	,CASE
		WHEN UPPER(pt.[form_sablonu_adi]) LIKE ('%2022%')																THEN '2022'
		ELSE '2023'
	END																													AS 'performance_year'
    ,UPPER(pt.[form_sablonu_adi])																						AS template_name
    ,UPPER(pt.[ulke/bolge])																								AS country
    ,CONCAT(UPPER(pt.[kisi_adi]), ' ', UPPER(pt.[kisi_soyadi]))															AS name_surname
    ,UPPER(pt.[bordro_sirketi])																							AS payroll_company
    ,UPPER(pt.[bordro_sirketi_kod])																					AS payroll_company_code
    ,CASE 
        WHEN  UPPER([calisma_yeri_turu]) = N'MERKEZ' THEN N'Head Office' 
        WHEN UPPER([calisma_yeri_turu])= N'İŞLETME' THEN N'Facilities'
        WHEN UPPER([calisma_yeri_turu])= N'IŞLETME' THEN N'Facilities'
        WHEN UPPER([calisma_yeri_turu])= N'ŞANTIYE' THEN N'Site'
    WHEN UPPER([calisma_yeri_turu])= N'ŞANTİYE' THEN N'Site' END  										                as workplace
    ,UPPER(pt.[kisi_sirket])																							AS personnel_subarea
    -- ,UPPER(pt.[gelir_seviyesi])																						AS ronesans_rank
    ,UPPER(pt.[ronesans_grade])																						    AS ronesans_rank
    ,UPPER(pt.[genel_performans_degerlendirmesi_aciklamasi_(dile_ozel)])												AS performance_rank_desc
    ,UPPER(pt.[genel_performans_degerlendirmesi])																		AS performance_rank
    ,CASE
        WHEN UPPER(pt.[genel_performans_degerlendirmesi]) = '1'															THEN 'UNSATISFACTORY'
        WHEN UPPER(pt.[genel_performans_degerlendirmesi]) = '2'															THEN 'BELOW EXPECTATIONS'
        WHEN UPPER(pt.[genel_performans_degerlendirmesi]) = '3'															THEN 'MEETS EXPECTATIONS'
        WHEN UPPER(pt.[genel_performans_degerlendirmesi]) = '4'															THEN 'EXCEEDS EXPECTATIONS'
        WHEN UPPER(pt.[genel_performans_degerlendirmesi]) = '5'															THEN 'SUBSTANTIALLY EXCEEDS EXPECTATIONS'
        ELSE 'UNASSESSED'
    END																													AS performance_rank_text
    ,UPPER(pt.[db_upload_timestamp])																					AS db_upload_timestamp

FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_performancetable') }} pt

    LEFT JOIN SICIL_RLS_MATCHING srm ON srm.employee_id = pt.kisi_personel_numarasi

WHERE [ulke/bolge] <> N'Rusya Federasyonu'
    AND pt.[genel_performans_degerlendirmesi] <> '__'
	