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
	[rls_region] = CASE WHEN (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = hr.[bordro_sirketi_code] COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR' ELSE 'RUS' END
	,[rls_group] = CONCAT(
							(SELECT TOP 1 [group] FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = hr.[bordro_sirketi_code] COLLATE DATABASE_DEFAULT)
							,'_',
							(CASE WHEN (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = hr.[bordro_sirketi_code] COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR' ELSE 'RUS' END)
						)
						
	,[rls_company] = CONCAT(
							hr.[bordro_sirketi_code],'_',
							(CASE WHEN (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = hr.[bordro_sirketi_code] COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR' ELSE 'RUS' END)
						)
	,[rls_businessarea] = NULL,
       UPPER(hr.[candidate_id])																				as candidate_id,
       UPPER(hr.[position_code])																			as position_code,
       UPPER(hr.[position_title])																			as position,
       UPPER(hr.[personel_area_code])																		as personnel_area_code,
	   UPPER(hr.[personel_area_name])																		as personnel_area,		
       UPPER(hr.[location_code])																			as location_code,
       UPPER(hr.[location_name])																			as location,
       UPPER(hr.[bordro_sirketi_code])																		as payroll_company_code,
       UPPER(hr.[bordro_sirketi_name])																		as payroll_company,
	   UPPER(hr.[job_req_id])																				as job_request_id,
	   UPPER(hr.[template_name])																			as templete_name,
	   UPPER(hr.[date_created])																				as creation_date,
	   UPPER(hr.[grup/baskanlik_name])																		as a_level,
       UPPER(ygf.[application_id])																			as application_id,
		CASE 
			WHEN  UPPER([calisma_yeri_turu]) = N'MERKEZ' THEN N'Head Office' 
			WHEN UPPER([calisma_yeri_turu])= N'İŞLETME' THEN N'Facilities'
			WHEN UPPER([calisma_yeri_turu])= N'IŞLETME' THEN N'Facilities'
			WHEN UPPER([calisma_yeri_turu])= N'ŞANTIYE' THEN N'Site'
		WHEN UPPER([calisma_yeri_turu])= N'ŞANTİYE' THEN N'Site' END  										as workplace,
	    CASE
			WHEN UPPER(ygf.[application_status]) LIKE '%TGF Gönderildi%'									then N'TGF'
			ELSE 'YGF'																						
		END																									as application_status,
		UPPER(ygf.[is_current_status])																		as is_current_status,
		UPPER(ygf.[skipped_status])																			as skipped_status,
		UPPER(ygf.[status_comment])																			as status_comment,
		UPPER(ygf.created_by)																				as created_personnel_sf_id,
		CONCAT(UPPER(ygf.[first_name]),' ',UPPER(ygf.[last_name]))											as created_full_name,
		CASE
			WHEN UPPER(ygf.[source_of_candidate's_cv_(detailed_information)]) LIKE N'%Linkedin%'			THEN 'LinkedIn'
			WHEN UPPER(ygf.[source_of_candidate's_cv_(detailed_information)]) LIKE N'%Kariyer%'				THEN 'Kariyer.net'
			WHEN UPPER(ygf.[source_of_candidate's_cv_(detailed_information)]) LIKE N'%R_nesansl_ Bilir%'	THEN 'Rönesanslı Bilir'
			WHEN UPPER(ygf.[source_of_candidate's_cv_(detailed_information)]) LIKE N'%mail%'				THEN 'E-mail'
			WHEN UPPER(ygf.[source_of_candidate's_cv_(detailed_information)]) LIKE N'%posta%'				THEN 'E-mail'
			WHEN UPPER(ygf.[source_of_candidate's_cv_(detailed_information)]) LIKE N'%Eski _al__an%'		THEN 'Previous Employee'
			ELSE 'Other'																				
		END																									as [source]
 
FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_hiringpositioncandidatetr') }} hr
	INNER JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_hiringcandidateygf') }} ygf ON hr.job_req_id = ygf.job_req_id
 
--UNION ALL
 
--SELECT 
--	[rls_region] = NULL --(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = REPLACE(RIGHT(iar.personel_area,4),')','') COLLATE DATABASE_DEFAULT)
--	,[rls_group] = NULL --CONCAT(
--						--	(SELECT TOP 1 [group] FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = REPLACE(RIGHT(iar.personel_area,4),')','') COLLATE DATABASE_DEFAULT)
--						--	,'_',
--						--	(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = REPLACE(RIGHT(iar.personel_area,4),')','') COLLATE DATABASE_DEFAULT) 
--						--)
--	,[rls_company] = NULL --CONCAT(REPLACE(RIGHT(iar.personel_area,4),')',''),'_',(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = REPLACE(RIGHT(iar.personel_area,4),')','') COLLATE DATABASE_DEFAULT) COLLATE DATABASE_DEFAULT ) 
--	,[rls_businessarea] = NULL,
--	   UPPER([candidate_id])							as candidate_id,
--       UPPER([position])								as position_name,
--       UPPER([job_title_(catalog)])						as job_title,
--       UPPER([personel_area])							as personnel_area,
--       UPPER([location])								as location,
--       UPPER([company])									as a_level,
--       UPPER([job_req_id])                              as job_request_ID,
--       UPPER([template_name])                           as template_name,
--       UPPER([date_created])                            as creation_date,
--       UPPER([e_mail_address])                          as candidate_mail,
--       UPPER([db_upload_timestamp])                     as db_upload_timestamp
--FROM [aws_stage].[hr_kpi].[raw__hr_kpi_t_dim_hiringpositioncandidateru] iar