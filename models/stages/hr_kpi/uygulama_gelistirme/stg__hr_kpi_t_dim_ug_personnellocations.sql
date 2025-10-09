{{
  config(
    materialized = 'table',tags = ['hr_kpi','hr_ug','personnel_locations'],grants = {'select': ['s4hana_ug_user']}
    )
}}


WITH blue_collar AS (

SELECT

	dwh_data_group = CASE
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
	,name = UPPER(FIRST_NAME)
	,surname = UPPER(LAST_NAME)
	,full_name = UPPER(CONCAT(FIRST_NAME,' ',LAST_NAME))
	,gender = GENDER
	,marital_status = MARITAL_STATUS_PICKLIST_LABEL
	,nationality = CASE
						WHEN nat_dim.country_three_digit IS NULL THEN NATIONALITY_LABEL
						ELSE nat_dim.country_three_digit 
				   END
	,payroll_company = CASE
							WHEN BORDRODURUMU = 'BORDROSUZ' THEN 'BORDROSUZ' 
							ELSE COMPANY_NAME
					   END
	,payroll_company_code = COMPANYCODE
	,company = COMPANY_NAME
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
	,c_level_region = BUSINESS_AREA
	,custom_region = (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group_sap] = A_SEVIYE_GRUPBASKANLIK COLLATE DATABASE_DEFAULT)
	,actual_working_country = awc_dim.country_three_digit
	,actual_working_city = COMPANY_CITY
	,collar_type = 	CASE
						WHEN YAKA_TUR = N'MAVI YAKA' THEN 'BLUE COLLAR'
						WHEN YAKA_TUR = N'MAVI YAKA' THEN 'BLUE COLLAR'
						WHEN YAKA_TUR = N'MAVİ YAKA' THEN 'BLUE COLLAR'
						WHEN YAKA_TUR = N'Mavı Yaka' THEN 'BLUE COLLAR'
						WHEN YAKA_TUR = N'Mavi Yaka' THEN 'BLUE COLLAR'
						WHEN YAKA_TUR = N'BEYAZ YAKA' THEN 'WHITE COLLAR'
						WHEN YAKA_TUR = N'BEYAZ YA' THEN 'WHITE COLLAR'
					  END
	/***** collar_type için TAŞERON BEYAZ YAKA PERSONELLERİ MEVCUT RONESANS BEYAZ YAKA İLE KARIŞMAMASI İÇİN MAVI YAKA OLARAK GÖSTERİLMİŞ, 
			TYPE KISMINDA AYRIŞTIRILMIŞTIR. *****/
				  -- CASE
						--WHEN YAKA_TUR = 'MAVI YAKA' THEN 'BLUE COLLAR'
						--WHEN YAKA_TUR = 'BEYAZ YAKA' THEN 'WHITE COLLAR'
				  -- END
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
	,dwh_date_of_termination = CASE WHEN (LEFT(ICIKISTARIH,4) = '9999' OR LEFT(ICIKISTARIH,4) = '0000') THEN NULL ELSE CAST(ICIKISTARIH AS DATE) END						
	,grouped_title = 'BLUE COLLAR' 
FROM {{ ref('stg__hr_kpi_t_dim_maviyaka_consolidated') }}  myc
	LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_country') }} nat_dim ON nat_dim.country_two_digit = myc.NATIONALITY_LABEL
	LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_country') }} awc_dim ON awc_dim.country_two_digit = myc.LAND1
)

SELECT
	dwh_data_group
	,employee_status
	,sf_id_number
	,employee_id
	,adines_number
	,global_id
	,sap_id
	,name
	,surname
	,full_name
	,gender
	,marital_status
	,nationality
	,payroll_company
	,payroll_company_code
	,company
	,employee_group
	,role_code
	,role
	,date_of_birth
	,age
	,language
	,a_level_group
	,b_level_company
	,c_level_region
	,d_level_department = c_level_region
	,custom_region
	,actual_working_country
	,actual_working_city
	,collar_type
	,dwh_workplace
	,dwh_origin_code
	,dwh_employee_type
	,dwh_education_status
	,dwh_date_of_recruitment
	,dwh_date_of_termination
	,grouped_title
FROM blue_collar ba

UNION ALL

SELECT
	dwh_data_group
	,employee_status
	,sf_id_number
	,employee_id
	,adines_number
	,global_id
	,sap_id
	,name
	,surname
	,full_name
	,gender
	,marital_status
	,nationality
	,payroll_company
	,payroll_company_code
	,company = b_level_company
	,employee_group
	,role_code
	,role
	,FORMAT(date_of_birth,'dd.MM.yyyy') date_of_birth
	,age
	,language
	,a_level_group
	,b_level_company
	,c_level_region
	,d_level_department
	,custom_region
	,actual_working_country
	,actual_working_city
	,collar_type
	,dwh_workplace
	,dwh_origin_code
	,dwh_employee_type
	,dwh_education_status
	,dwh_date_of_recruitment
	,dwh_date_of_termination
	,grouped_title
FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
WHERE language = 'EN' AND (grouped_title <> N'BLUE COLLAR' OR grouped_title IS NULL)