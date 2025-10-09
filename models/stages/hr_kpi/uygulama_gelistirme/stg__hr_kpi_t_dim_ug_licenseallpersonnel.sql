{{
  config(
    materialized = 'table',tags = ['hr_kpi','hr_ug','licenseallpersonnel'],grants = {'select': ['s4hana_ug_user']}
    )
}}
/*
	Beyaz yaka personel için hem eski sistemden hem de yeni sistemden gelen veriler birleştirilmistir.
	Bu tablo, Uygulama Geliştirme Ekibi tarafından lisans takibi amacıyla kullanılmaktadır.

	Kurguları şu şekildedir:
	Tablo içerisinden yalnızca aktif personel filtrelenir.
	Ekip, her gün gelen veriyi kendi taraflarında saklayarak bir önceki günün verisiyle karşılaştırma yapar.
	Eğer dün listede yer alan bir kişi, bugünkü veride yer almıyorsa, bu kişi çıkarılmış olarak değerlendirilir.
	Bu yaklaşım sayesinde sistemden çıkan kullanıcılar günlük olarak tespit edilebilmektedir.
*/
WITH union_data AS (
	SELECT 
		sf_system = 'Sftp'
		 ,employee_status = CASE WHEN sac.[employee_status] = '663908' THEN 'A' ELSE 'T' END
		 ,[user_id] as sf_id_number
		,[sac].person_id as employee_id
		,adines_id as adines_number
		,global_id = CAST(global_id AS NVARCHAR(MAX))
		,CAST(manager_global_id AS nvarchar) manager_global_id   
		,sap_id
		,[full_name] = CONCAT(CASE WHEN sac.middle_name IS NULL THEN UPPER(COALESCE(sac.first_name_lat,sac.first_name)) ELSE UPPER(CONCAT(COALESCE(sac.first_name_lat,sac.first_name),' ',COALESCE(sac.middle_name_lat,sac.middle_name))) END,' ',UPPER(COALESCE(sac.last_name_lat,sac.last_name)))
		,[username]
		,[a_level_group] = sac.[grup/baskanlik_en]
		,[b_level_company] = UPPER(sac.is_birimi_en)
		,[c_level_region] = UPPER([bolge/fonksiyon/bu_en])
		,[d_level_department] = UPPER(sac.[bolum/projeler/isletmeler_en])
		,[e_level_unit] = UPPER(sac.[birim_en])
		,actual_working_country = [sac].[country/region]
		,dwh_date_of_recruitment = CONVERT(DATETIME, sac.[sirket_ise_giris_tarihi], 104)
		,dwh_date_of_termination =  CONVERT(DATETIME, sac.[bitis_tarihi], 104)
		,'WHITE COLLAR' as collar_type
		,custom_region
		,[grup/baskanlik_kodu]
		,[is_birimi_kodu]
		,[bolge/fonksiyon/bu_kodu]
		,[bolum/projeler/isletmeler_kodu]
		,[birim_kodu]
		,[domain]
		,[eposta_adresi]
		,[bordro_sirketi_tr]
		,[bordro_sirketi_kodu]
		,_key = CONCAT(global_id, sap_id, user_id)
	FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_fact_sacreport') }} sac 
		LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }}  grp ON grp.[group] = sac.[grup/baskanlik_en]
	WHERE 1=1
		and sac.[employee_status] <> '663908'
		and custom_region <> 'RU'

UNION ALL

SELECT 
	sf_system = 'Rpeople'
	,CASE 
		WHEN employee_status_tr = N'AKTİF' THEN 'A'
		ELSE 'T'
	END AS employee_status
	,user_id AS sf_id_number
	,user_id AS employee_id
	,adines_number = NULL
	,global_id
	,CAST(manager_global_id AS nvarchar) AS manager_global_id
	,sap_id
	,CONCAT(name, ' ', surname) AS full_name
	,CASE 
		WHEN domain_user IS NULL OR domain_user = '' THEN NULL
		WHEN CHARINDEX('\', domain_user) > 0 THEN RIGHT(domain_user, LEN(domain_user) - CHARINDEX('\', domain_user))
		ELSE domain_user
	END AS username
	,a_level.name_en AS a_level_group
	,b_level.name_en AS b_level_company
	,c_level.name_en AS c_level_region 
	,d_level.name_en AS d_level_department
	,e_level.name_en AS e_level_unit
	,actual_working_country
	,[dwh_date_of_recruitment] = CASE WHEN emp.[roneans_last_start_date] IS NOT NULL THEN CAST(emp.[roneans_last_start_date] AS DATE) END
	,[dwh_date_of_termination] = 
								CASE 
									WHEN emp.job_end_date = '1753-01-01 00:00:00.000' 
									THEN NULL ELSE emp.job_end_date
								END
	,[collar_type] = 'WHITE COLLAR'
	,[custom_region] = (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = a_level.name_tr)
	,emp.a_level_code AS [grup/baskanlik_kodu]
	,emp.b_level_code AS is_birimi_kodu
	,emp.c_level_code AS [bolge/fonksiyon/bu_kodu]
	,emp.d_level_code AS [bolum/projeler/isletmeler_kodu]
	,emp.e_level_code AS [birim_kodu]
	,domain = domain_user
	,email_address AS eposta_adresi
	,payroll_company
	,payroll_company_code
	,_key = CONCAT(global_id, sap_id, user_id)
FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} emp
LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_a') }}  a_level ON a_level.code =emp.a_level_code
LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_b') }} b_level ON b_level.code = emp.[b_level_code]
LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_c') }} c_level ON c_level.code = emp.[c_level_code]
LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_d') }} d_level ON d_level.code = emp.[d_level_code]
LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_e') }} e_level ON e_level.code = emp.[e_level_code]
)

,white_collar AS (
	SELECT
		ROW_NUMBER() OVER (PARTITION BY _key ORDER BY sf_system asc) as rn
		,*
	FROM union_data
	)


SELECT DISTINCT 
	employee_status
	,sf_id_number
	,employee_id
	,adines_number
	,global_id
	,CASE 
		WHEN manager_global_id = 'None' THEN NULL
		ELSE CAST(manager_global_id AS float)
	END AS manager_global_id
	,sap_id
	,[full_name]
	,username
	,[a_level_group]
	,[b_level_company]
	,[c_level_region]
	,[d_level_department]
	,[e_level_unit]
	,actual_working_country
	,dwh_date_of_recruitment
	,dwh_date_of_termination
	,collar_type
	,custom_region
	,[grup/baskanlik_kodu]
	,[is_birimi_kodu]
	,[bolge/fonksiyon/bu_kodu]
	,[bolum/projeler/isletmeler_kodu]
	,[birim_kodu]
	,domain
	,[eposta_adresi] 
	,[bordro_sirketi_tr]
	,[bordro_sirketi_kodu]
FROM (
	SELECT 
		employee_status
		,sf_id_number
		,employee_id
		,adines_number
		,global_id
		,manager_global_id
		,sap_id
		,[full_name]
		,LOWER([username]) as username
		,[a_level_group]
		,[b_level_company]
		,[c_level_region]
		,[d_level_department]
		,[e_level_unit]
		,actual_working_country
		,dwh_date_of_recruitment
		,dwh_date_of_termination
		,collar_type
		,sac.custom_region
		,[grup/baskanlik_kodu]
		,[is_birimi_kodu]
		,[bolge/fonksiyon/bu_kodu]
		,[bolum/projeler/isletmeler_kodu]
		,[birim_kodu]
		,LOWER([domain]) as domain
		,LOWER([eposta_adresi]) as [eposta_adresi] 
		,[bordro_sirketi_tr]
		,[bordro_sirketi_kodu]
	FROM white_collar sac 
		LEFT JOIN  {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp ON grp.[group] = sac.[a_level_group]
	WHERE 1=1
		and sac.custom_region <> 'RU'
		and rn = 1 


	UNION ALL

	SELECT 
		STATU = CASE WHEN STATU = 'A' THEN 'A' WHEN STATU = 'P' THEN 'T' ELSE STATU END
		,[SF_SICIL_NO] = CAST(SF_SICIL_NO AS NVARCHAR(255))
		,employee_id = CAST(SF_SICIL_NO AS NVARCHAR(255))
		,adines_number = CAST(SF_SICIL_NO AS NVARCHAR(255))
		,global_id = CAST(GLOBAL_ID AS NVARCHAR(255))
		,NULL AS manager_global_id 
		,sap_id = CAST(SAP_ID AS NVARCHAR(255))
		,full_name = UPPER(CONCAT(FIRST_NAME,' ',LAST_NAME))
		,USERNAME
		,a_level_group = (SELECT TOP 1 [group] FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group_sap] = A_SEVIYE_GRUPBASKANLIK COLLATE DATABASE_DEFAULT)
		,b_level_company = CASE
								WHEN CALISAN_GRUP = 'T' THEN 'TAŞERON' 
								WHEN BORDRODURUMU = 'BORDROSUZ' THEN 'BORDROSUZ'
								ELSE COMPANY_NAME
						   END
		,c_level_region = CAST(NULL AS NVARCHAR(255)) --
		,d_level_department = CAST(NULL AS NVARCHAR(255)) --
		,e_level_unit = CAST(NULL AS NVARCHAR(255)) --
		,actual_working_country = awc_dim.country_three_digit
		,dwh_date_of_recruitment = CONVERT(DATETIME, BEGDA, 126)
		,dwh_date_of_termination =  '' 
		,collar_type = 'BLUE COLLAR'
		,custom_region = (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group_sap] = A_SEVIYE_GRUPBASKANLIK COLLATE DATABASE_DEFAULT)
		,[grup/baskanlik_kodu] = NULL
		,[is_birimi_kodu] = NULL
		,[bolge/fonksiyon/bu_kodu] = NULL
		,[bolum/projeler/isletmeler_kodu] = NULL
		,[birim_kodu] = NULL
		,[domain] = NULL
		,[eposta_adresi] = NULL
		,[bordro_sirketi_tr] = CASE
									WHEN BORDRODURUMU = 'BORDROSUZ' THEN 'BORDROSUZ' 
									ELSE COMPANY_NAME
								END
		,[bordro_sirketi_kodu] = CAST(NULL AS NVARCHAR(255)) 
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zhr_tr_mavi_yaka') }} my
		LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_country') }} nat_dim ON nat_dim.country_two_digit = my.NATIONALITY_LABEL
		LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_country') }} awc_dim ON awc_dim.country_two_digit = my.CUSTOM_REGION


	UNION ALL

	SELECT
		STATU = CASE WHEN ICIKISTARIH = '00000000' THEN 'A' ELSE 'T' END
		,[SF_SICIL_NO] = CAST(SF_SICIL_NO AS NVARCHAR(255))
		,employee_id = CAST(SF_SICIL_NO AS NVARCHAR(255))
		,adines_number = CAST(SF_SICIL_NO AS NVARCHAR(255))--
		,global_id = CAST(GLOBAL_ID AS NVARCHAR(255))
		,NULL AS manager_global_id
		,sap_id = CAST(SAP_ID AS NVARCHAR(255))
		,full_name = UPPER(CONCAT(FIRST_NAME,' ',LAST_NAME))
		,USERNAME = ''
		,a_level_group = (SELECT TOP 1 [group] FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group_sap] = A_SEVIYE_GRUPBASKANLIK COLLATE DATABASE_DEFAULT)
		,b_level_company = CASE
								WHEN CALISAN_GRUP = 'T' THEN 'TAŞERON' 
								WHEN BORDRODURUMU = 'BORDROSUZ' THEN 'BORDROSUZ'
								ELSE COMPANY_NAME
						   END
		,c_level_region = CAST(NULL AS NVARCHAR(255))
		,d_level_department = CAST(B_SEVIYE_GRUPBASKANLIK AS NVARCHAR(255)) --BOLUM
		,e_level_unit = CAST(NULL AS NVARCHAR(255))
		,actual_working_country = awc_dim.country_three_digit
		,dwh_date_of_recruitment = CONVERT(DATETIME, IGIRISTARIH, 126)
		,dwh_date_of_termination =  CASE WHEN ICIKISTARIH = '0000-00-00' THEN NULL ELSE CONVERT(DATETIME, ICIKISTARIH, 126) END
		,collar_type = 'BLUE COLLAR'
		,custom_region = (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group_sap] = A_SEVIYE_GRUPBASKANLIK COLLATE DATABASE_DEFAULT)
		,[grup/baskanlik_kodu] = NULL
		,[is_birimi_kodu] = NULL
		,[bolge/fonksiyon/bu_kodu] = NULL
		,[bolum/projeler/isletmeler_kodu] = NULL
		,[birim_kodu] = NULL
		,[domain] = NULL
		,[eposta_adresi] = NULL
		,[bordro_sirketi_tr] = CASE
									WHEN BORDRODURUMU = 'BORDROSUZ' THEN 'BORDROSUZ' 
									ELSE COMPANY_NAME
								END
		,[bordro_sirketi_kodu] = CAST(NULL AS NVARCHAR(255)) 
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zhr_tr_maviyakac') }} my 
		LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_country') }} nat_dim ON nat_dim.country_two_digit = my.NATIONALITY_LABEL
		LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_country') }} awc_dim ON awc_dim.country_two_digit = my.LAND1
) HR_TABLE_UNION