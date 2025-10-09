{{
  config(
    materialized = 'view',tags = ['eff_kpi']
    )
}}
SELECT

	rls_region = CASE 
					WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = [a_level_group]) = 'TR' THEN 'TUR'
					ELSE 'RUS' 
				 END
	,rls_group = CONCAT(
						UPPER((SELECT TOP 1 group_rls FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = [a_level_group])),'_',
						CASE 
							WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = [a_level_group]) = 'TR' THEN 'TUR'
							ELSE 'RUS' 
						END)
	,rls_company = CONCAT(UPPER(payroll_company_code),'_',
						CASE 
							WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = [a_level_group]) = 'TR' THEN 'TUR'
							ELSE 'RUS' 
						END)
	,rls_businessarea = CONCAT(dwh_worksite,'_',
						CASE 
							WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = [a_level_group]) = 'TR' THEN 'TUR'
							ELSE 'RUS' 
						END)
	,a.[ENVANTERID]					as [inventory_id]	
    ,a.[STOKID]						as [stock_id]
    ,a.[GSICIL]						as [global_id]
    ,a.[SICIL]						as [domain_name]
	,b.full_name
	,b.employee_id	
	,b.sf_id_number					as [user_id]
	,b.a_level_group
	,b.[payroll_company_code]		as [payroll_company_code]
	,b.[dwh_worksite]				as [business_area]
    ,a.[ACIKLAMA]					as [description]
    ,a.[KAYIT_YAPAN]				as [register_user]
    ,a.[KAYIT_ZAMAN]				as [register_date]
    ,a.[ZIMMETTESLİM]				as [entrusted]
    ,a.[KONTROLEDILDI]				as [verified]
    ,a.[ENVANTERDOSYA]				as [inventory_field]
    ,a.[TALIMATDOSYA]				as [instruction_field]
    ,a.[ULKEKODU]					as [country_code]
    ,a.[SIRKET]						as [company]
    ,a.[TANIM]						as [item]
    ,a.[ISLETIMSIS]					as [operating_system]
    ,a.[OFFICE]						as [office]
    ,a.[KAYIT_DRM]					as [registration_status]
    ,a.[BTM_MARKA]					as [brand]
    ,a.[BTM_MODEL]					as [model]
    ,a.[BTM_EKRAN]					as [screen_inch]
    ,a.[BTM_HDD]					as [hdd]
    ,a.[BTM_CPU]					as [cpu]
    ,a.[BTM_RAM]					as [ram]
    ,a.[BTM_VGA]					as [vga]
    ,a.[BTM_OPTIC]					as [optic]
	,b.[dwh_date_of_recruitment]	as [recruitment_date]
FROM [PRODAPPSDB].[RNS_DBRONESANS_PROD].[dbo].[DW_VIEW_RaporZimmet] a
	INNER JOIN 
				(
					SELECT * FROM {{ ref('dm__hr_kpi_t_dim_hrall') }} where language='EN'
				) b ON a.GSICIL COLLATE DATABASE_DEFAULT = b.global_id

	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON b.[payroll_company_code] = kuc.RobiKisaKod collate database_default