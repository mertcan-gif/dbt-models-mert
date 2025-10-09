{{
  config(
    materialized = 'table',tags = ['hr_kpi','maviyaka','maviyakafinalized']
    )
}}

SELECT
	rls_region = CASE 
					WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group_sap] = A_SEVIYE_GRUPBASKANLIK COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR'
					ELSE 'RUS' 
				END
	,rls_group = CONCAT(
						(SELECT TOP 1 [group_rls] FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group_sap] = A_SEVIYE_GRUPBASKANLIK COLLATE DATABASE_DEFAULT) ,'_',
						CASE 
							WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group_sap] = A_SEVIYE_GRUPBASKANLIK COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR'
							ELSE 'RUS' 
						END
						)
	,rls_company =  CONCAT(
						COMPANYCODE,'_',
						CASE 
							WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group_sap] = A_SEVIYE_GRUPBASKANLIK COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR'
							ELSE 'RUS' 
						END
						)
	,rls_businessarea = CONCAT(
						'_',
						CASE 
							WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group_sap] = A_SEVIYE_GRUPBASKANLIK COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR'
							ELSE 'RUS' 
						END
						) --BTRTL???
	,dwh_data_group = CASE
						WHEN BORDRODURUMU = 'BORDROSUZ' THEN 'NO PAYROLL'
						WHEN YAKA_TUR = N'MAVI YAKA' THEN 'BLUE COLLAR'
						WHEN YAKA_TUR = N'MAVI YAKA' THEN 'BLUE COLLAR'
						WHEN YAKA_TUR = N'MAVİ YAKA' THEN 'BLUE COLLAR'
						WHEN YAKA_TUR = N'Mavı Yaka' THEN 'BLUE COLLAR'
						WHEN YAKA_TUR = N'Mavi Yaka' THEN 'BLUE COLLAR'
						WHEN YAKA_TUR = N'BEYAZ YAKA' THEN 'WHITE COLLAR'
						WHEN YAKA_TUR = N'BEYAZ YA' THEN 'WHITE COLLAR'
					  END
	,employee_status = CASE
							WHEN STATU = 'P' THEN 'T'
							WHEN STATU = 'A' THEN 'A'
							WHEN STATU IS NULL AND ICIKISTARIH = '0000-00-00' THEN 'A'
							WHEN STATU IS NULL AND ICIKISTARIH <> '0000-00-00' THEN 'T'
						END
	,sf_id_number = CAST(SF_SICIL_NO AS NVARCHAR(255))
	,employee_id = CAST(SF_SICIL_NO AS NVARCHAR(255))
	,adines_number = CAST(SF_SICIL_NO AS NVARCHAR(255))
	,global_id = CAST(GLOBAL_ID AS NVARCHAR(255)) 
	,sap_id = CAST(SAP_ID AS NVARCHAR(255)) 
	,username = CAST(NULL AS NVARCHAR(255))
	,name = UPPER(FIRST_NAME)
	,surname = UPPER(LAST_NAME)
	,full_name = UPPER(CONCAT(FIRST_NAME,' ',LAST_NAME))
	,gender = GENDER
	,marital_status = MARITAL_STATUS_PICKLIST_LABEL
	,nationality = CASE
						WHEN nat_dim.country_three_digit IS NULL THEN NATIONALITY_LABEL
						ELSE nat_dim.country_three_digit 
				   END
	,event_reason = 'MAVI YAKA'
	,country = CAST(NULL AS NVARCHAR(255))
	,payroll_company = CASE
							WHEN BORDRODURUMU = 'BORDROSUZ' THEN 'BORDROSUZ' 
							ELSE COMPANY_NAME
					   END
	,cost_center = CAST(NULL AS NVARCHAR(255))
	,employee_group = CASE
					      WHEN YAKA_TUR = N'BEYAZ YAKA' OR YAKA_TUR = N'BEYAZ YA' THEN 'WHITE COLLAR SUBCONTRACTOR'
						  WHEN CALISAN_GRUP = '1' THEN 'OTHER'
						  WHEN CALISAN_GRUP = '2' THEN 'LOCAL'
						  WHEN CALISAN_GRUP = '3' THEN 'EXPAT'
						  WHEN CALISAN_GRUP = 'T' THEN 'BLUE COLLAR SUBCONTRACTOR'
						  ELSE CALISAN_GRUP
					  END
	,role_code = JOB_CLASSIFICATION_EXTERNAL_CO
	,role = JOB_CLASSIFICATION_LABEL
	,ronesans_job_level = CAST(NULL AS NVARCHAR(255))
	,supervisor_name = CASE
							WHEN BORDRODURUMU = N'BORDROSUZ' THEN 'NO PAYROLL'
							ELSE 'BLUE COLLAR'
						END 
	,date_of_birth = CASE 
						WHEN LEN(DATE_OF_BIRTH) = 8
							 AND LEFT(DATE_OF_BIRTH,2) > 17
							 AND RIGHT(LEFT(DATE_OF_BIRTH,6),2) <= 12 
							 AND RIGHT(DATE_OF_BIRTH,2) <= 31
							 AND RIGHT(DATE_OF_BIRTH,2) > 0
							THEN CONVERT(NVARCHAR(10), CONVERT(DATE, DATE_OF_BIRTH, 112), 104)
						ELSE NULL
					 END
	,age =  CASE 
						WHEN LEN(DATE_OF_BIRTH) = 8
							 AND LEFT(DATE_OF_BIRTH,2) > 17
							 AND RIGHT(LEFT(DATE_OF_BIRTH,6),2) <= 12 
							 AND RIGHT(DATE_OF_BIRTH,2) <= 31
							 AND RIGHT(DATE_OF_BIRTH,2) > 0
							THEN DATEDIFF(DD,DATE_OF_BIRTH,GETDATE())/365
						ELSE NULL
					 END
	,language = 'EN'
	,a_level_group = (SELECT TOP 1 [group] FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group_sap] = A_SEVIYE_GRUPBASKANLIK COLLATE DATABASE_DEFAULT)
	,b_level_company = CASE
							WHEN CALISAN_GRUP = 'T' THEN 'TAŞERON' 
							WHEN BORDRODURUMU = N'BORDROSUZ' THEN 'BORDROSUZ'
							ELSE COMPANY_NAME
					   END
	,c_level_region = CAST(NULL AS NVARCHAR(255))
	,d_level_department = CAST(NULL AS NVARCHAR(255))
	,e_level_unit = CAST(NULL AS NVARCHAR(255))
	,custom_region = (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group_sap] = A_SEVIYE_GRUPBASKANLIK COLLATE DATABASE_DEFAULT)
	,actual_working_country = awc_dim.country_three_digit
	,actual_working_city = COMPANY_CITY
	,collar_type = /***** 'BLUE COLLAR'
				collar_type için TAŞERON BEYAZ YAKA PERSONELLERİ MEVCUT RONESANS BEYAZ YAKA İLE KARIŞMAMASI İÇİN MAVI YAKA OLARAK GÖSTERİLMİŞ, 
				TYPE KISMINDA AYRIŞTIRILMIŞTIR.
				2024-11-25 Numan: Employment Type olarak değiştirildiği için
				 *****/
					   CASE
							WHEN YAKA_TUR = 'MAVI YAKA' OR YAKA_TUR = N'MAVİ YAKA' OR YAKA_TUR = N'Mavı Yaka' THEN 'BLUE COLLAR'
							WHEN YAKA_TUR = 'BEYAZ YAKA' THEN 'WHITE COLLAR'
					   END
	,dwh_leave = CAST(NULL AS NVARCHAR(255))
	,dwh_annual_leave = CAST(NULL AS NVARCHAR(255))
	,dwh_used_leave = CAST(NULL AS NVARCHAR(255))
	,dwh_workplace = CASE
						WHEN CALISAN_GRUP = 'T' THEN 'SUBCONTRACTOR'
						WHEN YAKA_TUR = N'MAVI YAKA' THEN 'Blue Collar'
						WHEN YAKA_TUR = N'MAVI YAKA' THEN 'Blue Collar'
						WHEN YAKA_TUR = N'MAVİ YAKA' THEN 'Blue Collar'
						WHEN YAKA_TUR = N'Mavı Yaka' THEN 'Blue Collar'
						WHEN YAKA_TUR = N'Mavi Yaka' THEN 'Blue Collar'
						ELSE NULL
					 END 
	,dwh_origin_code = (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group_sap] = A_SEVIYE_GRUPBASKANLIK COLLATE DATABASE_DEFAULT)
	,dwh_type = CAST(NULL AS NVARCHAR(255))
	,dwh_employee_type = CASE WHEN BORDRODURUMU = 'BORDROSUZ' THEN 'BORDROSUZ' ELSE NULL END
	,dwh_education_status = CASE
								WHEN YAKA_TUR = N'MAVI YAKA' THEN 'BLUE COLLAR'
								WHEN YAKA_TUR = N'MAVI YAKA' THEN 'BLUE COLLAR'
								WHEN YAKA_TUR = N'MAVİ YAKA' THEN 'BLUE COLLAR'
								WHEN YAKA_TUR = N'Mavı Yaka' THEN 'BLUE COLLAR'
								WHEN YAKA_TUR = N'Mavi Yaka' THEN 'BLUE COLLAR'
								ELSE ESEVIYESI
							END
	,dwh_date_of_recruitment = CAST(IGIRISTARIH AS DATE)
							--	CASE 
							--		WHEN LEN(IGIRISTARIH) = 8
							--				AND LEFT(IGIRISTARIH,2) > 17
							--				AND RIGHT(LEFT(IGIRISTARIH,6),2) <= 12 
							--				AND RIGHT(IGIRISTARIH,2) <= 31
							--				AND RIGHT(IGIRISTARIH,2) > 0
							--			THEN CAST(IGIRISTARIH AS DATE)
							--		ELSE NULL
							--	END 
	,dwh_date_of_termination = CASE WHEN (LEFT(ICIKISTARIH,4) = '9999' OR LEFT(ICIKISTARIH,4) = '0000') THEN NULL ELSE CAST(ICIKISTARIH AS DATE) END
							--	CASE 
							--		WHEN LEN(ICIKISTARIH) = 8
							--				AND LEFT(ICIKISTARIH,2) > 17
							--				AND RIGHT(LEFT(ICIKISTARIH,6),2) <= 12 
							--				AND RIGHT(ICIKISTARIH,2) <= 31
							--				AND RIGHT(ICIKISTARIH,2) > 0
							--			THEN CAST(ICIKISTARIH AS DATE)
							--		ELSE NULL
							--	END 							
	,ronesans_rank = '99'
	,grouped_title = 'BLUE COLLAR' 
	,dwh_termination_reason = CASE
								WHEN BORDRODURUMU = 'BORDROSUZ' THEN 'NO PAYROLL'
								WHEN YAKA_TUR = N'MAVI YAKA' THEN 'BLUE COLLAR'
								WHEN YAKA_TUR = N'MAVI YAKA' THEN 'BLUE COLLAR'
								WHEN YAKA_TUR = N'MAVİ YAKA' THEN 'BLUE COLLAR'
								WHEN YAKA_TUR = N'Mavı Yaka' THEN 'BLUE COLLAR'
								WHEN YAKA_TUR = N'Mavi Yaka' THEN 'BLUE COLLAR'
							  END
	,dwh_cause_details = CASE
							WHEN BORDRODURUMU = 'BORDROSUZ' THEN 'NO PAYROLL'
							WHEN YAKA_TUR = N'MAVI YAKA' THEN 'BLUE COLLAR'
							WHEN YAKA_TUR = N'MAVI YAKA' THEN 'BLUE COLLAR'
							WHEN YAKA_TUR = N'MAVİ YAKA' THEN 'BLUE COLLAR'
							WHEN YAKA_TUR = N'Mavı Yaka' THEN 'BLUE COLLAR'
							WHEN YAKA_TUR = N'Mavi Yaka' THEN 'BLUE COLLAR'
						 END
	,dwh_ronesans_last_seniority = CAST(NULL AS NVARCHAR(255))
	,dwh_ronesans_seniority = CAST(NULL AS NVARCHAR(255))
	,dwh_non_ronesans_seniority = CAST(NULL AS NVARCHAR(255))
	,dwh_ronesans_total_seniority = CAST(NULL AS NVARCHAR(255))
	,dwh_actual_termination_reason = CAST(NULL AS NVARCHAR(255))
	,dwh_worksite = CAST(NULL AS NVARCHAR(255))
	,payroll_sub_unit = CAST(NULL AS NVARCHAR(255))
	,dwh_worksite_description = CAST(NULL AS NVARCHAR(255))
	,dwh_workplace_merged = CASE
								WHEN YAKA_TUR = N'MAVI YAKA' THEN 'BLUE COLLAR'
								WHEN YAKA_TUR = N'MAVI YAKA' THEN 'BLUE COLLAR'
								WHEN YAKA_TUR = N'MAVİ YAKA' THEN 'BLUE COLLAR'
								WHEN YAKA_TUR = N'Mavı Yaka' THEN 'BLUE COLLAR'
								WHEN YAKA_TUR = N'Mavi Yaka' THEN 'BLUE COLLAR'
								ELSE NULL
							END
	,KodBordroSirketi = CAST(NULL AS NVARCHAR(255))
	,photo_base64 = ''
	,employment_type = CASE 
						WHEN BORDRODURUMU = 'BORDROLU' AND CALISAN_GRUP<>'T' THEN N'Rönesans'
						ELSE N'Subcontractor'
						END
FROM {{ ref('stg__hr_kpi_t_dim_maviyaka_consolidated') }} myc
	LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_country') }} nat_dim ON nat_dim.country_two_digit = myc.NATIONALITY_LABEL
	LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_country') }} awc_dim ON awc_dim.country_two_digit = myc.LAND1

