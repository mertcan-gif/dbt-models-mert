{{
  config(
    materialized = 'view',tags = ['eff_kpi']
    )
}}


WITH DISTINCT_SAP_USER_IDS AS (
	SELECT DISTINCT
		rls_region = CASE 
						WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = [grup/baskanlik_en]) = 'TR' THEN 'TUR'
						ELSE 'RUS' 
					 END
		,rls_group = UPPER((SELECT TOP 1 group_rls FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = [grup/baskanlik_en]))
		,[company] = UPPER(bordro_sirketi_kodu)
		,[businessarea] = externalcode_picklistoption
		,[sap_id]
		,[user_id]	
		,[global_id]
	FROM  {{ source('stg_hr_kpi', 'raw__hr_kpi_t_fact_sacreport') }}
),

EDUCATION_LEVELS AS (
	SELECT * FROM (
	SELECT
		kisi_taniticisi
		,egitim_seviyesi_kod
		,egitim_seviyesi
		,RN = ROW_NUMBER() OVER(PARTITION BY kisi_taniticisi ORDER BY egitim_seviyesi_kod DESC)
	FROM   {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_educationreport') }}
	) ORDERED_DATA
	WHERE RN = 1
)

,UNION_DATA AS (

	SELECT
		s.rls_region
		,rls_group = CONCAT(s.rls_group,'_',s.rls_region)
		,rls_company = CONCAT(s.[company],'_',s.rls_region)
		,rls_businessarea = CONCAT(s.[businessarea],'_',s.rls_region)
		,[global_id] = CAST(s.[global_id] AS NVARCHAR)
		,s.sap_id
		,user_system_id = i.user_sys_id
		,full_name = CONCAT(i.first_name,' ',i.last_name)
		,[experience_company] = 'RON'
		,i.[company]
		,s.[company] AS company_code
		,s.[businessarea] AS business_area
		,[group] = i.company
		,[position] = lower(eff_kpi.fn_TurkishToEnglish(lower(cast(i.title as varchar(max)))))
		,[position_starting_date] = CAST(i.from_date AS DATE)-- CONVERT(DATE, CONVERT(VARCHAR, RIGHT(i.from_date,4)) + '-' + CONVERT(VARCHAR, LEFT(RIGHT(i.from_date,7),2)) + '-' + CONVERT(VARCHAR, LEFT(i.from_date,2)))
		,[position_ending_date] = CAST(i.bitis_tarihi AS DATE) --CONVERT(DATE, CONVERT(VARCHAR, RIGHT(i.bitis_tarihi,4)) + '-' + CONVERT(VARCHAR, LEFT(RIGHT(i.bitis_tarihi,7),2)) + '-' + CONVERT(VARCHAR, LEFT(i.bitis_tarihi,2)))
		,[position_duration] = CASE
									WHEN CAST(i.bitis_tarihi AS DATE) <= GETDATE() 
										THEN DATEDIFF(D,CAST(i.from_date AS DATE), CAST(i.bitis_tarihi AS DATE))
									ELSE DATEDIFF(D,CAST(i.from_date AS DATE), GETDATE())
							   END
		,[language] = CASE 
						  WHEN (CONCAT(l.okuma,l.konusma,l.yazma) LIKE N'%Advanced%' OR CONCAT(l.okuma,l.konusma,l.yazma) LIKE N'%İleri%') THEN 'İleri'
						  WHEN CONCAT(l.okuma,l.konusma,l.yazma) LIKE N'%İyi%' THEN 'İyi'
						  ELSE '-'
						END
		,[education] = e.egitim_seviyesi
	FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_insideworkexperience') }}  i 
		LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_language') }}  l ON l.userid = i.user_sys_id  and l.dil = N'İngilizce'
		--LEFT JOIN [aws_stage].[hr_kpi].[raw__hr_kpi_t_dim_promotiontable] p ON p.sf_id = i.user_sys_id
		LEFT JOIN DISTINCT_SAP_USER_IDS s ON s.[user_id] = i.user_sys_id
		LEFT JOIN EDUCATION_LEVELS e ON e.kisi_taniticisi = s.[user_id]
		where s.sap_id IS NOT NULL

	UNION ALL

	SELECT
		s.rls_region
		,rls_group = CONCAT(s.rls_group,'_',s.rls_region)
		,rls_company = CONCAT(s.[company],'_',s.rls_region)
		,rls_businessarea = CONCAT(s.[businessarea],'_',s.rls_region)
		,[global_id] = CAST(s.[global_id] AS NVARCHAR)
		,s.sap_id
		,i.userid
		,full_name = i.adsoyad
		,[experience_company] = 'NON-RON'
		,i.[sirket]
		,s.[company] AS company_code
		,s.[businessarea] AS business_area
		,[group] = ''
		,[position] = lower(eff_kpi.fn_TurkishToEnglish(lower(cast(i.pozisyon as varchar(max)))))
		,[position_starting_date] = CONVERT(DATE, CONVERT(VARCHAR, RIGHT(i.baslangic_tarihi,4)) + '-' + CONVERT(VARCHAR, LEFT(RIGHT(i.baslangic_tarihi,7),2)) + '-' + CONVERT(VARCHAR, LEFT(i.baslangic_tarihi,2)))
		,[position_ending_date] = CONVERT(DATE, CONVERT(VARCHAR, RIGHT(i.bitis_tarihi,4)) + '-' + CONVERT(VARCHAR, LEFT(RIGHT(i.bitis_tarihi,7),2)) + '-' + CONVERT(VARCHAR, LEFT(i.bitis_tarihi,2)))
		,[position_duration] = CASE
									WHEN CONVERT(DATE, CONVERT(VARCHAR, RIGHT(i.bitis_tarihi,4)) + '-' + CONVERT(VARCHAR, LEFT(RIGHT(i.bitis_tarihi,7),2)) + '-' + CONVERT(VARCHAR, LEFT(i.bitis_tarihi,2))) <= GETDATE() 
										THEN DATEDIFF(D,CONVERT(DATE, CONVERT(VARCHAR, RIGHT(i.baslangic_tarihi,4)) + '-' + CONVERT(VARCHAR, LEFT(RIGHT(i.baslangic_tarihi,7),2)) + '-' + CONVERT(VARCHAR, LEFT(i.baslangic_tarihi,2))), CONVERT(DATE, CONVERT(VARCHAR, RIGHT(i.bitis_tarihi,4)) + '-' + CONVERT(VARCHAR, LEFT(RIGHT(i.bitis_tarihi,7),2)) + '-' + CONVERT(VARCHAR, LEFT(i.bitis_tarihi,2))))
									ELSE DATEDIFF(D,CONVERT(DATE, CONVERT(VARCHAR, RIGHT(i.baslangic_tarihi,4)) + '-' + CONVERT(VARCHAR, LEFT(RIGHT(i.baslangic_tarihi,7),2)) + '-' + CONVERT(VARCHAR, LEFT(i.baslangic_tarihi,2))), GETDATE())
							   END
		,[language] = CASE 
						  WHEN (CONCAT(l.okuma,l.konusma,l.yazma) LIKE N'%Advanced%' OR CONCAT(l.okuma,l.konusma,l.yazma) LIKE N'%İleri%') THEN 'İleri'
						  WHEN CONCAT(l.okuma,l.konusma,l.yazma) LIKE N'%İyi%' THEN 'İyi'
						  ELSE '-'
						END
		,[education] = e.egitim_seviyesi
	FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_outsideworkexperience') }} i 
		LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_language') }} l ON l.userid = i.userid and l.dil = N'İngilizce'
		--LEFT JOIN [aws_stage].[hr_kpi].[raw__hr_kpi_t_dim_promotiontable] p ON p.sf_id = i.userid
		LEFT JOIN DISTINCT_SAP_USER_IDS s ON s.[user_id] = i.userid
		LEFT JOIN EDUCATION_LEVELS e ON e.kisi_taniticisi = s.[user_id]
		where s.sap_id IS NOT NULL
)



SELECT * FROM UNION_DATA