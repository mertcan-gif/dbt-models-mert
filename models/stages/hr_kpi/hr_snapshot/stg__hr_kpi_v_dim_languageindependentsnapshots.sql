{{
  config(
    materialized = 'view',tags = ['hr_kpi','hr_snapshots']
    )
}}

/***
Dökümantasyon:
Tarih: 2023-31-05
Yazan: Kaan Keskin

Power BI'a ve AWS'e gitmeden önce ECSAC tablosu ile aldığımız personel listesini
	-EN
	-TR
	-RU
olacak şekilde her bir personeli 3 dilde kırmaktayız, bazı kolonlar 3 dilde de aynı bazıları ise farklı yazılmaktadır.
Bu view üç dilde de ''AYNI'' yazılan kolonları içermektedir!

Burada oluşturulan bir personel için kolonlar dilden bağımsız olduğu için tek bir satır gelmektedir., [hr_kpi].[DWH_Stage_hr_kpiV1_v_SAC_LanguageDependent] view'u ile birleştirdiğimizde
nihai view'a ulaşmaktayız. 
Not:
- [hr_kpi].[DWH_Stage_hr_kpiV1_v_SAC_LanguageIndpendent] 'viewunda tek bir kişi bir kez tekrar ederken
- [hr_kpi].[DWH_Stage_hr_kpiV1_v_SAC_LanguageDependent] 'viewunda bir kişi 3 kez tekrar eder, bu iki view join edildiğinde bir kişi 3 kez her dil için tekrar etmektedir.
language filtresinden filtrelenerek istenilen veriye ulaşılabilir.
İki kolonu birleştiren key kolonu: [sf_id_number] yani [user_id]'dir
***/


WITH consolidated_data AS (
SELECT 
	rls_region = CASE 
					WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = sac.[grup/baskanlik_en]) = 'TR' THEN 'TUR'
					ELSE 'RUS' 
				 END
	,rls_group = UPPER((SELECT TOP 1 group_rls FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = sac.[grup/baskanlik_en]))
	,rls_company = UPPER([sac].bordro_sirketi_kodu)
	,rls_businessarea = [sac].externalcode_picklistoption
	,employee_status = CASE WHEN sac.[employee_status] = '663908' THEN 'A' ELSE 'T' END
	,sf_id_number = [sac].user_id
	,employee_id = [sac].person_id
	,adines_number = [sac].adines_id
	,global_id = [sac].global_id
	,sap_id = [sac].sap_id
	,username = UPPER([sac].username)
	,gender = [sac].gender_personal_information
	,marital_status = UPPER([sac].marital_status_personal_information)
	,nationality = [sac].nationality_personal_information
	,country = [sac].[country/region]
	,role_code = [sac].gorev_kodu
	,ronesans_job_level = CAST([sac].ronesans_job_level AS int)
	,supervisor_name = CONCAT(CASE WHEN sac.[manager_middle_name] IS NULL THEN UPPER(sac.[manager_first_name]) ELSE UPPER(CONCAT(sac.[manager_first_name], ' ',sac.[manager_middle_name])) END,' ',UPPER(sac.[manager_last_name]))
	,date_of_birth = [sac].date_of_birth_biographical_information
	,age = dbo.get_age([sac].date_of_birth_biographical_information)
	,language = UPPER('en')
	,custom_region = (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = sac.[grup/baskanlik_en])
	,actual_working_country = [sac].[country/region]
	,actual_working_city = UPPER([sac].personel_alt_alani_sehir_us)
	,dwh_leave = COALESCE(izin.[DWH_AnnualLeave], 0) + COALESCE(izin.[DWH_UsedLeave] , 0)
	,dwh_annual_leave = izin.[DWH_AnnualLeave]
	,dwh_used_leave = izin.[DWH_UsedLeave]
	,dwh_origin_code = (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = sac.[grup/baskanlik_en])
	,dwh_type = 'SF'
	,dwh_employee_type = CASE
							WHEN ((sac.[calisan_tipi] = '' OR sac.[calisan_tipi] IS NULL)
									AND (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = sac.[grup/baskanlik_en]) = 'TR' --custom_region
									AND sac.[country/region_of_birth_biographical_information] <> 'TUR' 
								) THEN 'Normal'
							ELSE sac.[calisan_tipi] 
						  END
	,dwh_date_of_recruitment = CASE
									WHEN rrdd.global_id IS NULL THEN CONVERT(DATETIME, sac.[sirket_ise_giris_tarihi], 104)
									ELSE rrdd.recruitment_date
							   END
	,dwh_date_of_termination =  CONVERT(DATETIME, sac.[bitis_tarihi], 104)
	,ronesans_rank = [sac].ronesans_kademe
	,grouped_title = UPPER(ug.Gruplanmis_Unvan_ENG)
	,dwh_termination_reason = CASE
								WHEN (1=1 
									AND sac.[employee_status] = '663918' 
									AND trmn.termination_category = N'INVOL-TERMINATION OF EMPLOYMENT' 
									AND CONVERT(DATETIME, sac.[bitis_tarihi], 104) < GETDATE())
								THEN N'INVOL-TERMINATION OF EMPLOYMENT'
								WHEN (1=1
									AND sac.[employee_status] = '663918' 
									AND trmn.termination_category = N'VOL-RESIGNATION'
									AND CONVERT(DATETIME, sac.[bitis_tarihi], 104) < GETDATE())
								THEN N'VOL-RESIGNATION'
								WHEN (1=1
									AND sac.[employee_status] = '663918'
									AND trmn.termination_category = N'TRANSFER'
									AND CONVERT(DATETIME, sac.[bitis_tarihi], 104) < GETDATE()) 
								THEN N'TRANSFER'
								WHEN (1=1
									AND sac.[employee_status] = '663918' 
									AND UPPER(sac.[gercek_isten_cikis_sebebi]) = '' 
									AND	CONVERT(DATETIME, sac.[bitis_tarihi], 104) < GETDATE()) 
								THEN N'NULL'
							   ELSE N'ACTIVE' END
	,dwh_cause_details = CASE WHEN sac.[employee_status] = '663918' THEN UPPER(sac.[gercek_isten_cikis_sebebi])	ELSE N'ACTIVE' 	END
	,dwh_ronesans_last_seniority = 	CASE
										WHEN sac.[employee_status] = '663908'	THEN DATEDIFF(DAY, CONVERT(DATETIME, sac.[sirket_ise_giris_tarihi], 104), GETDATE())
										ELSE DATEDIFF(DAY, CONVERT(DATETIME, sac.[sirket_ise_giris_tarihi], 104), CONVERT(DATETIME, sac.[bitis_tarihi], 104)) 
									END
	,dwh_ronesans_seniority = 0
	,dwh_non_ronesans_seniority = 0
	,dwh_ronesans_total_seniority = 0
	,dwh_actual_termination_reason = UPPER(sac.[gercek_isten_cikis_sebebi])
	,dwh_worksite = [sac].externalcode_picklistoption
	,payroll_sub_unit = [sac].bordro_alt_birimi
	,dwh_worksite_description = UPPER(sac.[label_picklistlabel])
	,payroll_company_code = [sac].bordro_sirketi_kodu
	,snapshot_date
FROM {{ source('snapshots_hr_kpi', 'DWH_Stage_SF_SACRapor_Snapshots') }} sac
	LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_titlegroup') }}  ug on sac.[ronesans_job_level] = ug.Ronesans_Job_Level -- şimdilik AWS'den aldım
	LEFT JOIN {{ ref('stg__hr_kpi_v_dim_usedremainingleave') }} AS izin on sac.[global_id] = CAST(izin.merge_id AS nvarchar)
	LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_terminationreasons') }} trmn ON UPPER(trmn.reason_for_termination) = UPPER(sac.[gercek_isten_cikis_sebebi])	 
	LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_realrecruitmentdatesdebug') }} rrdd ON sac.global_id = rrdd.global_id
WHERE 1=1
	AND DAY(DATEADD(D,1,sac.snapshot_date)) = '01'
	AND (sac.[calisan_tipi] NOT IN (N'Oryantasyon') OR sac.[calisan_tipi] IS NULL)
	AND (
		sac.bordro_sirketi_tr <> (N'Snh-Rec Adi Ortaklığı')
		AND (
				(
					sac.cost_center_tr NOT IN ('REC TEMETTÜ MÜH.ÇÖZ','KZA HOTEL','KZA OTEL',N'KALE TEKNIK-ŞANTIYE','KALE TEKNIK-ŞANTIYE','KALE TECHNICAL-SITE','Rec Temettu Muh.Çoz','Rec Temettü Müh.Çöz','Personel Grup-2 SAREN') 	
					OR sac.cost_center_en NOT IN ('REC TEMETTÜ MÜH.ÇÖZ','KZA HOTEL','KZA OTEL',N'KALE TEKNIK-ŞANTIYE','KALE TEKNIK-ŞANTIYE','KALE TECHNICAL-SITE','Rec Temettu Muh.Çoz','Rec Temettü Müh.Çöz','Personel Grup-2 SAREN')
					OR sac.cost_center_ru NOT IN ('REC TEMETTÜ MÜH.ÇÖZ','KZA HOTEL','KZA OTEL',N'KALE TEKNIK-ŞANTIYE','KALE TEKNIK-ŞANTIYE','KALE TECHNICAL-SITE','Rec Temettu Muh.Çoz','Rec Temettü Müh.Çöz','Personel Grup-2 SAREN')
				)
				OR sac.cost_center_tr IS NULL
			)
		AND (sac.gorev_tanimi_tr NOT LIKE N'%STAJYER%' OR sac.gorev_tanimi_tr IS NULL)
		AND (sac.calisan_tipi <> 'Ghost User' OR sac.calisan_tipi IS NULL)
		)
	AND (
			sac.[grup/baskanlik_en] <> N'DESNA'
			OR sac.[grup/baskanlik_ru] <> N'DESNA'
			OR sac.[grup/baskanlik_tr] <> N'DESNA'
		)
    AND (
			sac.[grup/baskanlik_en] <> N'ENERGO HEAD QUARTERS'
			OR sac.[grup/baskanlik_ru] <> N'ENERGO HEAD QUARTERS'
			OR sac.[grup/baskanlik_tr] <> N'ENERGO HEAD QUARTERS'
		)
	AND 1 = CASE
				WHEN sac.employee_status = '663918' 
					AND	CONVERT(DATETIME, sac.[bitis_tarihi], 104) >= '2022-01-01' 
				THEN 1
				WHEN sac.employee_status = '663908' THEN 1
				ELSE 0
			END
	AND sac.[user_id] NOT IN (SELECT * FROM {{ ref('stg__hr_kpi_t_dim_concurrentusers') }})
)

SELECT
	rls_region 
	,rls_group = CONCAT(COALESCE([rls_group],''),'_',COALESCE([rls_region],''))
	,rls_company = CONCAT(COALESCE([rls_company],''),'_',COALESCE([rls_region],''))
	,rls_businessarea = CONCAT(COALESCE([rls_businessarea],''),'_',COALESCE([rls_region],''))
	,employee_status
	,sf_id_number
	,employee_id
	,adines_number 
	,global_id
	,sap_id
	,username
	,gender
	,marital_status
	,nationality
	,country
	,role_code
	,ronesans_job_level
	,supervisor_name
	,date_of_birth
	,age
	,language 
	,custom_region
	,actual_working_country
	,actual_working_city
	,dwh_leave
	,dwh_annual_leave
	,dwh_used_leave
	,dwh_origin_code
	,dwh_type
	,dwh_employee_type
	,dwh_date_of_recruitment
	,dwh_date_of_termination
	,ronesans_rank
	,grouped_title
	,dwh_termination_reason
	,dwh_cause_details
	,dwh_ronesans_last_seniority
	,dwh_ronesans_seniority
	,dwh_non_ronesans_seniority
	,dwh_ronesans_total_seniority
	,dwh_actual_termination_reason
	,dwh_worksite
	,payroll_sub_unit
	,dwh_worksite_description
	,payroll_company_code
	,snapshot_date
FROM consolidated_data


GO


