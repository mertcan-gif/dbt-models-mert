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
			,payroll_company_code
			,dwh_worksite
			,ROW_NUMBER() OVER(PARTITION BY employee_id ORDER BY event_reason,age DESC) AS RN	
		FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
		WHERE language = 'EN'
	) RAW_DATA
	WHERE RAW_DATA.RN = 1
)

SELECT
			   srm.rls_region,
			   srm.rls_group,
			   srm.rls_company,
			   srm.rls_businessarea,
			CASE WHEN
			   UPPER([calisan_statusu]) = '663908'	THEN 'A'
			 ELSE 'T' END													          as employee_status,
			   UPPER([user_id])												      as user_id,
			   UPPER([sf_id])												        as sf_id,
			   UPPER([global_id])											      as global_id,
			   UPPER([sap_id])												      as sap_id,
			   CONCAT(UPPER([ad]), ' ', UPPER([soyad]))			as name_surname,
			   UPPER([pozisyon_kodu])										    as position_code,
			   UPPER([is_kodu_tr])											    as position_name_tr,
			   UPPER([is_kodu_en])											as position_name_en,
			   UPPER([is_fonksiyonu_tr])									as title_tr,
			   UPPER([grup/baskanlik_tr])									as a_level_tr,
			   UPPER([grup/baskanlik_en])									as a_level_en,
			   srm.payroll_company_code,
			   srm.dwh_worksite												as business_area,
			   UPPER([sirket_ise_giris_tarihi])								as company_hire_date,
			   UPPER([grup_baslangic_tarihi])								as group_start_date,
			   UPPER([kidem_baslangic_tarihi])								as seniority_start_date,
			   UPPER([yillik_izin_baz_tarihi])								as annual_leave_start_date,
			   UPPER([mevcut_bordro_sirketi_nakil_tarihi])					as current_payroll_company_transfer_date,
			   UPPER([sgk_icin_engellilik_durumu])							as disability_status_sgk,
			   UPPER([bitis_tarihi])										as termination_date,
			   UPPER([etkinlik_etkinlik_nedeni])							as event_reason,
			   UPPER([etkinlik_nedeni_adi_etkinlik_nedeni])					as event_name,
			   UPPER([start_date])											as start_date,
			   UPPER([db_upload_timestamp])									as db_upload_timestamp

FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_promotiontable') }} tt
  	LEFT JOIN SICIL_RLS_MATCHING srm ON srm.employee_id = tt.user_id