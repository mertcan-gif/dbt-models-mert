{{
  config(
    materialized = 'table',tags = ['hr_kpi','personnelcard']
    )
}}

WITH MAZERET_IZIN AS(
    SELECT
        SFI.[global_id]
        ,SUM(SFD.[yazilan_sure]) as excuse_leave
    FROM aws_stage.hr_kpi.raw__hr_kpi_t_fact_timeaccountdetail AS SFD
        LEFT OUTER JOIN aws_stage.hr_kpi.raw__hr_kpi_t_fact_timeaccount AS SFI ON SFI.[harici_kod]=SFD.[zaman_hesabi_harici_kod]
    WHERE SFD.[yazma_birimi]='HOURS' AND [yazma_tarihi]<=GETDATE()
	GROUP BY 
        SFI.[global_id]
),

PERSONNEL_DATA AS (
SELECT    
    rls_region = CASE 
					WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = sac.[grup/baskanlik_en]) = 'TR' THEN 'TUR'
					ELSE 'RUS' 
				 END
	,rls_group = CONCAT(COALESCE(UPPER((SELECT TOP 1 group_rls FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = sac.[grup/baskanlik_en])),''),'_'
				,COALESCE(CASE 
								WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = sac.[grup/baskanlik_en]) = 'TR' THEN 'TUR'
								ELSE 'RUS' 
							END,''))
	,rls_company = CONCAT(COALESCE(UPPER(sac.is_birimi_en),''),'_',
					COALESCE(CASE 
								WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = sac.[grup/baskanlik_en]) = 'TR' THEN 'TUR'
								ELSE 'RUS' 
							END,''))
	,rls_businessarea = CONCAT(COALESCE([sac].externalcode_picklistoption,''),'_',
						COALESCE(CASE 
									WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = sac.[grup/baskanlik_en]) = 'TR' THEN 'TUR'
									ELSE 'RUS' 
								END,''))
	,grouped_title = UPPER(ug.Gruplanmis_Unvan_ENG)
	,[dwh_workplace_merged] = UPPER(sac.rapor_organizasyon_us)
	,actual_working_city = UPPER([sac].personel_alt_alani_sehir_us)
	,actual_working_country = [sac].[country/region]
	,local_time = ''
	,manager = CONCAT(CASE WHEN sac.[manager_middle_name] IS NULL THEN UPPER(sac.[manager_first_name]) ELSE UPPER(CONCAT(sac.[manager_first_name], ' ',sac.[manager_middle_name])) END,' ',UPPER(sac.[manager_last_name]))
	,second_manager = CONCAT(act.second_manager_name,' ',act.second_manager_surname) 
	,hr_representative = CONCAT(act.hr_responsible_name,' ',act.hr_responsible_surname)
	,position = act.position
	,position_code = act.position_code
	,position_entry_date = act.hire_date --?
	,time_in_position = CASE 
							WHEN act.employee_status = N'AKTİF' THEN DATEDIFF(D,CAST(GETDATE() AS DATE),CAST(act.hire_date AS DATE))
							ELSE DATEDIFF(D,CAST(act.hire_date AS DATE),CAST(act.termination_date AS DATE))
						END
	,act.termination_date
	,employee_group_act = act.employee_group --dosyada iki adet employee_group vardı. Biri HRALL biri ACTPER'den eşlenen. ikisini de ekledim, buna şimdilik _act ekledim
	,turkish_identification_number = act.employee_national_id
	,person_personnel_id = sac.person_id
	,adines_id = CAST(sac.adines_id as nvarchar)
	,global_id = CAST(sac.global_id as nvarchar)
	,sac.sap_id
	,sf_id = sac.[user_id] -- üstteki user_id ile aynı
	,date_of_birth = sac.date_of_birth_biographical_information
	,place_of_birth = ''
	,country_of_birth = sac.[country/region_of_birth_biographical_information]
	,salutation = ''
	,nationality = sac.nationality_personal_information
	,second_nationality = sac.second_nationality_personal_information
	,third_nationality = ''
	,age = aws_stage.dbo.get_age(sac.date_of_birth_biographical_information)
	,gender = sac.gender_personal_information
	,marital_status = sac.marital_status_personal_information
	,native_preferred_language = ''
	,blood_type = ''
	,payroll_company = UPPER(sac.bordro_sirketi_tr) --snapshot olarak yazılmış ama snapshot verisi de sac'dan besleniyor. o da buradan alıyor olarak gördüm
	,company = UPPER(sac.is_birimi_en)
	,division = CAST(sac.[bolge/fonksiyon/bu_kodu] as nvarchar)
	,department = UPPER(sac.[birim_en])
	,personnel_area = ''
	,personnel_subarea = CONCAT(act.employee_sub_area_code,' - ',act.employee_sub_area)
	,cost_center = CONCAT(sac.cost_center_en,' (',sac.costcenter_code,')')
	,sub_division = UPPER(sac.[bolum/projeler/isletmeler_en])
	,grup_baskanlik = sac.[grup/baskanlik_en] --promotion table'da yazıyordu fakat orada veri çokluyor. sac'da da güncel hali var gibi görülüyor
	,leave_info = COALESCE(izin.[DWH_AnnualLeave], 0) + COALESCE(izin.[DWH_UsedLeave] , 0)
	,annual_leave = COALESCE(izin.[DWH_AnnualLeave], 0) 
	,used_leave =  -1*(COALESCE(izin.[DWH_UsedLeave] , 0))	
	,excuse_leave = mi.excuse_leave
	,reporting_relation = ''
	,family_info = ''
	,education_info = egt.egitim_seviyesi_en
	,company_hire_date = CASE
								WHEN rrdd.global_id IS NULL THEN CONVERT(DATETIME, sac.[sirket_ise_giris_tarihi], 104)
								ELSE rrdd.recruitment_date
							END --dwh_date_of_recruitment
	,group_start_date = '' -- promotion tablosunda unique değil kişiler. buraya eklersek çoklayacaktır
	,seniority_start_date = '' -- promotion tablosunda unique değil kişiler. buraya eklersek çoklayacaktır
	,is_contingent_worker = ''
	,annual_leave_base_date = '' -- sac'daki izinden de alınabilir mi? promotionda tekrar ettiğinden
	,entry_date_to_social_security = ''
	,retirement_date = ''
	,transfer_date_to_currenct_legal_entity = '' -- promotion tablosunda unique değil kişiler. buraya eklersek çoklayacaktır
	,assignment_id = ''
	,employee_status = CASE WHEN sac.[employee_status] = '663908' THEN 'A' ELSE 'T' END
	,country_region = [sac].[country/region]
	,physical_location = UPPER([sac].personel_alt_alani_sehir_us)
	,supervisor = CONCAT(CASE WHEN sac.[manager_middle_name] IS NULL THEN UPPER(sac.[manager_first_name]) ELSE UPPER(CONCAT(sac.[manager_first_name], ' ',sac.[manager_middle_name])) END,' ',UPPER(sac.[manager_last_name]))
	,job_classification = CONCAT(UPPER(sac.gorev_tanimi_en),' (',sac.gorev_kodu,')')
	,local_job_title = UPPER(sac.gorev_tanimi_en) -- SF'ten de bakılacak
	,global_job_title = UPPER(sac.gorev_tanimi_en) -- SF'ten de bakılacak
	,regular_temporary = CASE
							WHEN ((sac.[calisan_tipi] = '' OR sac.[calisan_tipi] IS NULL)
									AND (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = sac.[grup/baskanlik_en]) = 'TR' --custom_region
									AND sac.[country/region_of_birth_biographical_information] <> 'TUR' 
								) THEN 'Normal'
							ELSE sac.[calisan_tipi] 
						  END
	,employee_group = UPPER(sac.calisangrubu_r_en)
	,employee_type = CASE
						WHEN ((sac.[calisan_tipi] = '' OR sac.[calisan_tipi] IS NULL)
								AND (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = sac.[grup/baskanlik_en]) = 'TR' --custom_region
								AND sac.[country/region_of_birth_biographical_information] <> 'TUR' 
							) THEN 'Normal'
						ELSE sac.[calisan_tipi] 
					  END 
	,employee_subgroup = sac.employee_sub_group_en
	,pay_grade = CASE 
					WHEN [sac].gelir_seviyesi_revize IS NULL THEN CAST([sac].gelir_seviyesi AS nvarchar)
					ELSE CAST([sac].gelir_seviyesi_revize AS nvarchar)
				END
	,ronesans_job_level =  CAST([sac].ronesans_job_level AS int)
	,past_experiences_ron = '' -- promotion tablosunda unique değil kişiler. buraya eklersek çoklayacaktır
	,past_experiences_non_ron = '' -- outsideworkexp tablosunda unique değil kişiler. buraya eklersek çoklayacaktır
	,promotion = '' -- promotion ve insideworkexp tablolarında unique değil kişiler. buraya eklersek çoklayacaktır
	,performance_year = '' -- performance tablosuna 2023 ve 2024 eklenince veri çoklayacaktır
	,workplace = '' -- performance tablosuna 2023 ve 2024 eklenince veri çoklayacaktır
	,performance = '' -- performance tablosuna 2023 ve 2024 eklenince veri çoklayacaktır
	,rehired_information = '' --[dm__eff_kpi_v_dim_terminationandrehire] tablosu çokluyor
	,ticket = ''
FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_fact_sacreport') }} sac
	LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_titlegroup') }}  ug on sac.[ronesans_job_level] = ug.Ronesans_Job_Level
	LEFT JOIN {{ ref('stg__hr_kpi_v_sf_activepersonnel') }} act ON act.user_id = sac.sap_id
	LEFT JOIN {{ ref('stg__hr_kpi_v_dim_usedremainingleave') }} AS izin on sac.[global_id] = izin.merge_id
	LEFT JOIN {{ ref('stg__hr_kpi_v_dim_maxeducation') }}  egt ON egt.person_id = sac.person_id
	LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_realrecruitmentdatesdebug') }} rrdd ON sac.global_id = rrdd.global_id
	LEFT JOIN MAZERET_IZIN mi ON mi.global_id = sac.global_id
WHERE 1=1 
	-- AND sac.sap_id = '47064618'
	-- AND sac.[employee_status] = '663908'
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

SELECT * 
FROM PERSONNEL_DATA pd
WHERE rls_region <> 'RUS'

    