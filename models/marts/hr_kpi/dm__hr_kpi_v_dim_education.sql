{{
  config(
    materialized = 'view',tags = ['hr_kpi','education']
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
    ,global_id = e.kisi_taniticisi
    ,education_level_code = e.egitim_seviyesi_kod
    ,education_level = e.egitim_seviyesi
    ,school = e.okul
    ,department = e.bolum
    ,starting_date = e.baslangic_tarihi
    ,graduation_year = e.bitis_tarihi
    ,score_gpa = e.[bitirme_derecesi_/_skala] 
    ,e.db_upload_timestamp
FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_educationreport') }} e
    LEFT JOIN SICIL_RLS_MATCHING srm ON srm.employee_id = e.kisi_taniticisi