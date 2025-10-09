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
	[rls_region] = CASE WHEN (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = iat.kod_bordro_sirketi COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR' ELSE 'RUS' END
	,[rls_group] = CONCAT(
							(SELECT TOP 1 [group] FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = iat.kod_bordro_sirketi COLLATE DATABASE_DEFAULT)
							,'_',
							(CASE WHEN (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = iat.kod_bordro_sirketi COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR' ELSE 'RUS' END)
						)
						
	,[rls_company] = CONCAT(
							iat.kod_bordro_sirketi,'_',
							(CASE WHEN (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = iat.kod_bordro_sirketi COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR' ELSE 'RUS' END)
						)
	,[rls_businessarea] = NULL,
	   --UPPER([kod_bordro_sirketi])				as rls_company,
       UPPER([is_talebi_numarasi])																									as job_request_ID,
       UPPER([pozisyon_kodu])																										as position_code,
       UPPER([pozisyon])																											as position_name,
       UPPER([template_name])																										as template_name,
       UPPER([sirket_adi])																											as company,
       UPPER([acilma_tarihi])																										as opening_date,
       UPPER([kapanma_tarihi])																										as closing_date,
       UPPER([kapatma_nedeni])																										as closing_reason,
       UPPER([durum])																												as status,
       UPPER([label_picklistlabel])																									as label_picklistlabel,
	   UPPER([silinen])																												as deletion_status,
	   UPPER([kod_bordro_sirketi])																									as payroll_company_code,
	   CONCAT(UPPER([operator_first_name_jobrequisitionoperator]),' ',UPPER([operator_last_name_jobrequisitionoperator]))			as created_full_name,
	   UPPER([countryofregistration_bordro_sirketi])																				as country,
	   UPPER([grup/baskanlik])																										as a_level,
       CASE 
            WHEN  UPPER([calisma_yeri_turu]) = N'MERKEZ' THEN N'Head Office' 
            WHEN UPPER([calisma_yeri_turu])= N'İŞLETME' THEN N'Facilities'
            WHEN UPPER([calisma_yeri_turu])= N'IŞLETME' THEN N'Facilities'
            WHEN UPPER([calisma_yeri_turu])= N'ŞANTIYE' THEN N'Site'
       WHEN UPPER([calisma_yeri_turu])= N'ŞANTİYE' THEN N'Site' END  										                        as workplace,
      UPPER([db_upload_timestamp])	                                                                                                as db_upload_timestamp

FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_hiringpositiontr') }} iat
WHERE silinen = 'Not Deleted'

--UNION ALL

--SELECT 
--	[rls_region] = (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = iar.kod_bordro_sirketi COLLATE DATABASE_DEFAULT)
--	,[rls_group] = CONCAT(
--							(SELECT TOP 1 [group] FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = iar.kod_bordro_sirketi COLLATE DATABASE_DEFAULT)
--							,'_',
--							(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = iar.kod_bordro_sirketi COLLATE DATABASE_DEFAULT) 
--						)
--	,[rls_company] = CONCAT(iar.kod_bordro_sirketi ,'_',(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = iar.kod_bordro_sirketi COLLATE DATABASE_DEFAULT) COLLATE DATABASE_DEFAULT ) 
--	,[rls_businessarea] = NULL,
--	   --UPPER([kod_bordro_sirketi])				as rls_company,
--	   UPPER([is_talebi_numarasi])				as job_request_ID,
--       UPPER([pozisyon_kodu])					as position_code,
--       UPPER([pozisyon])						as position_name,
--       UPPER([template_name])					as template_name,
--       UPPER([sirket])                          as company,
--       UPPER([acilma_tarihi])                   as opening_date,
--       UPPER([kapanma_tarihi])                  as closing_date,
--       UPPER([kapatma_nedeni])                  as closing_reason,
--       UPPER([durum])							as status,
--       UPPER([label_picklistlabel])             as label_picklistlabel,
--	   UPPER([is_deleted])						as deletion_status,
--       UPPER([db_upload_timestamp])             as db_upload_timestamp

--FROM [aws_stage].[hr_kpi].[raw__hr_kpi_t_dim_hiringpositionru] iar
--WHERE is_deleted = 'Not Deleted'