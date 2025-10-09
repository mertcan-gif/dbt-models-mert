{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}
WITH hr_blue_collar_raw_data AS (
	SELECT
		CASE
			WHEN YAKA_TUR = 'MAVI YAKA' OR YAKA_TUR = N'MAVİ YAKA' OR YAKA_TUR = N'Mavı Yaka' THEN 'BLUE COLLAR'
			WHEN YAKA_TUR = 'BEYAZ YAKA' THEN 'WHITE COLLAR'
		END AS collar_type 
		,CASE
			WHEN STATU = 'P' THEN 'TERMINATED'
			WHEN STATU = 'A' THEN 'ACTIVE'
			WHEN STATU IS NULL AND ICIKISTARIH = '0000-00-00' THEN 'ACTIVE'
			WHEN STATU IS NULL AND ICIKISTARIH <> '0000-00-00' THEN 'TERMINATED'
		END AS [employee_status]
		,[SF_SICIL_NO] as sf_id_number
		,FIRST_NAME as name
		,LAST_NAME as lastname
		,CONCAT(FIRST_NAME, ' ', LAST_NAME) as full_name 
		,gender
		,companycode as company_code
		,company_name
		,awc_dim.country_three_digit as actual_working_country
		,CASE 
			WHEN BORDRODURUMU = 'BORDROLU' AND CALISAN_GRUP<>'T' THEN N'Rönesans'
			ELSE N'Subcontractor'
		END AS employment_type
		,source = N'sap_blue_collar'
	FROM {{ ref('stg__hr_kpi_t_dim_maviyaka_consolidated') }}  mv
	LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_country') }} awc_dim ON awc_dim.country_two_digit = mv.LAND1
	)

,union_data as (
	SELECT 
		cm.RegionCode rls_region
		,CONCAT(cm.KyribaGrup, '_', cm.RegionCode) rls_group
		,CONCAT(cm.RobiKisaKod, '_', cm.RegionCode)	rls_company
		,CONCAT('_', cm.RegionCode) rls_businessarea
		,collar_type = 'WHITE COLLAR'
		,CASE
			WHEN [employee_status_tr] = N'AKTİF' THEN 'ACTIVE'
			WHEN [employee_status_tr] = N'ÇIKARILMIŞ' THEN 'TERMINATED'
		END AS [employee_status]
		,sf_id_number = e.[user_id] 
		,[name]
		,[surname]
		,full_name = CONCAT(name, ' ', surname)
		,gender
		,company_code = payroll_company_code
		,company_name = payroll_company
		,e.actual_working_country
		,employment_type_tr = 'Rönesans'
		,source = 'sf'
	FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} e
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on cm.RobiKisaKod = e.payroll_company_code
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_a') }}  levela on e.a_level_code = levela.code
		WHERE 1=1
			AND (e.employee_type_name_en NOT IN (N'Oryantasyon',N'Ghost User') OR e.employee_type_name_en IS NULL)
			AND (
				e.payroll_company <> (N'Snh-Rec Adi Ortaklığı')
				AND (
					cost_center_name NOT IN (N'REC TEMETTÜ MÜH.ÇÖZ','KZA HOTEL','KZA OTEL',N'KALE TEKNIK-ŞANTIYE','KALE TEKNIK-ŞANTIYE','KALE TECHNICAL-SITE','Rec Temettu Muh.Çoz',N'Rec Temettü Müh.Çöz','Personel Grup-2 SAREN') 	
					OR 
					cost_center_name IS NULL
					)
				AND (e.position NOT LIKE N'%STAJYER%' OR e.position IS NULL)
				)
			AND (
					levela.name_tr <> N'DESNA' 
					OR levela.name_en <> N'DESNA' --employess tablosunda böyle bir kayıt yok. indepented sorgusunda olduğu için ekledim.
				) 
			AND (
					levela.name_tr <> N'ENERGO HEAD QUARTERS' 
					OR levela.name_en <> N'ENERGO HEAD QUARTERS' --employess tablosunda böyle bir kayıt yok. indepented sorgusunda olduğu için ekledim.
				) 
			AND employee_status_tr = N'AKTİF' --sadece aktif olanlar filtrelenmiştir.

	UNION ALL

	SELECT 
		rls_region
		,rls_group
		,rls_company
		,CONCAT('_', cm.RegionCode) AS rls_businessarea
		,collar_type
		,[employee_status]
		,sf_id_number
		,name
		,lastname
		,full_name
		,gender
		,company_code
		,company_name
		,actual_working_country
		,employment_type
		,source
	FROM hr_blue_collar_raw_data rd
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on cm.RobiKisaKod = rd.company_code
		WHERE employee_status = 'ACTIVE'

	UNION ALL

	SELECT
		rls_region = 'EUR'
		,rls_group = 'BNGROUP_EUR'
		,rls_company = 'NS_BLN_EUR'
		,rls_businessarea = '_EUR'
		,CASE
			WHEN Collar_Type = 'White Collar' THEN 'WHITE COLLAR'
			WHEN Collar_Type = 'Blue Collar' THEN 'BLUE COLLAR'
		END AS collar_type
		,Employee_Status = 'ACTIVE'
		,Global_Employee_ID
		,Name
		,Surname
		,full_name = CONCAT(Name, ' ', Surname)
		,Gender
		,payroll_company = 'Ballast Europe'
		,payroll_company_name = 'Ballast Europe'
		,Actual_Working_Country
		,employment_type = 'Ballast'
		,source = 'ballast'
	FROM [PRDSYNDW-ONDEMAND.SQL.AZURESYNAPSE.NET].[ronesansdwh].[dbo].[fact_HumanResourcesRonesans]
		WHERE Employee_Status = 'A'
)

SELECT
	ud.*
FROM union_data ud
	WHERE 1=1
		AND (ud.rls_region <> 'RUS' or ud.rls_region IS NULL)