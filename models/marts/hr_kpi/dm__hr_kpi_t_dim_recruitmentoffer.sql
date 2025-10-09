{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}


WITH CompanyUnionMappingTable AS 
(
    SELECT 
        company,
		[group] = group_rls,
        region = CASE 
					WHEN dg.custom_region = 'NA' THEN 'CLO' 
					WHEN dg.custom_region IS NULL THEN 'RU'
					ELSE dg.custom_region END
    FROM  (
    SELECT KyribaKisaKod AS company,KyribaGrup AS [group],RegionCode AS region FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }}
    ) raw_data
    LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} dg ON dg.[group] = raw_data.[group]

)

SELECT 

	[rls_region] = CASE WHEN (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = pt.sirket COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR' ELSE 'RUS' END
	,[rls_group] = CONCAT(
							(SELECT TOP 1 [group] FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = pt.sirket COLLATE DATABASE_DEFAULT)
							,'_',
							(CASE WHEN (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = pt.sirket COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR' ELSE 'RUS' END)
						)
						
	,[rls_company] = CONCAT(
							sirket,'_',
							(CASE WHEN (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = pt.sirket COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR' ELSE 'RUS' END)
						)
	,[rls_businessarea] = NULL,
       UPPER([candidate_id])						as candidate_id,
       [kod_pozisyon]       						as position_code,
       UPPER([baslik_pozisyon])						as position_name,
       UPPER([ad_personel_alani])					as personnel_subarea,
       UPPER([ad_personel_alani.1])					as personnel_area,
       UPPER([teklif_tarihi])						as offer_date,
       UPPER([teklif_durum])						as offer_status,
       CASE
            WHEN UPPER([teklif_ret_sebebi]) = N'AILEVI NEDENLER DOLAYISIYLA' THEN N'FAMILY-RELATED REASONS'
            WHEN UPPER([teklif_ret_sebebi]) = N'BAŞKA BIR FIRMA İLE ANLAŞMASI NEDENIYLE' THEN N'DUE TO AGREEMENT WITH ANOTHER COMPANY'
            WHEN UPPER([teklif_ret_sebebi]) = N'ÇALIŞMA ŞARTLARI NEDENIYLE (ÇALIŞMA SAATLERI, OFIS-ŞANTIYE AYRIMI VB)' THEN N'DUE TO WORKING CONDITIONS'
            WHEN UPPER([teklif_ret_sebebi]) = N'DIĞER NEDENLER' THEN N'OTHER REASONS'
            WHEN UPPER([teklif_ret_sebebi]) = N'LOKASYON NEDENIYLE' THEN N'DUE TO LOCATION'
            WHEN UPPER([teklif_ret_sebebi]) = N'MEVCUT FIRMASINDA DEVAM KARARI ALMASI NEDENIYLE' THEN N'DECISION TO CONTINUE WITH CURRENT COMPANY'
            WHEN UPPER([teklif_ret_sebebi]) = N'POZISYONU YETERSIZ BULMASI NEDENIYLE' THEN N'PERCEIVED INADEQUACY OF POSITION'
            WHEN UPPER([teklif_ret_sebebi]) = N'ÜCRET NEDENIYLE' THEN N'DUE TO SALARY'
            WHEN UPPER([teklif_ret_sebebi]) = N'YAN HAKLARIN YETERSIZ OLMASI NEDENIYLE (KONAKLAMA-ULAŞIM-ÖZEL SAĞLIK SIGORTASI VS.)' THEN N'INSUFFICIENT ADDITIONAL BENEFITS'
            ELSE UPPER([teklif_ret_sebebi])
        END                                         as offer_rejection_reason,
       UPPER([sirket])								as company_code,
	   UPPER([adi_bordro_sirketi])					as payroll_company,
       UPPER([istalebi])							as job_request,
       UPPER([aday_mail])							as candidate_mail,
       UPPER([template_name])						as template_name,
       UPPER([is_deleted_jobrequisition])			as deletion_status,
       UPPER([db_upload_timestamp])					as db_upload_timestamp

FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_hiringpositionoffertr') }} pt
WHERE [is_deleted_jobrequisition] = 'Not Deleted'
        
--UNION ALL

--SELECT
--	[rls_region] = (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = ptr.sirket COLLATE DATABASE_DEFAULT)
--	,[rls_group] = CONCAT(
--							(SELECT TOP 1 [group] FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = ptr.sirket COLLATE DATABASE_DEFAULT)
--							,'_',
--							(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = ptr.sirket COLLATE DATABASE_DEFAULT) 
--						)
--	,[rls_company] = CONCAT(sirket,'_',(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = ptr.sirket COLLATE DATABASE_DEFAULT) COLLATE DATABASE_DEFAULT ) 
--	,[rls_businessarea] = NULL,
--       UPPER([candidate_id])						as candidate_id,
--       UPPER([kod_pozisyon])						as position_code,
--       UPPER([baslik_pozisyon])						as position_name,
--       UPPER([ad_personel_alt_alani])				as personnel_subarea,
--       UPPER([ad_personel_alani])					as personnel_area,
--       UPPER([teklif_tarihi])						as offer_date,
--       UPPER([teklif_durum])						as offer_status,
--       UPPER([teklif_ret_sebebi])					as offer_rejection_reason,
--       UPPER([sirket])								as company_code,
--	   UPPER([adi_bordro_sirketi])					as payroll_company,
--       UPPER([istalebi])							as job_request,
--       UPPER([aday_mail])							as candidate_mail,
--       UPPER([template_name])						as template_name,
--       UPPER([is_deleted_jobrequisition])			as deletion_status,
--       UPPER([db_upload_timestamp])					as db_upload_timestamp

--FROM [aws_stage].[hr_kpi].[raw__hr_kpi_t_dim_hiringpositionofferru] ptr
--WHERE [is_deleted_jobrequisition] = 'Not Deleted'