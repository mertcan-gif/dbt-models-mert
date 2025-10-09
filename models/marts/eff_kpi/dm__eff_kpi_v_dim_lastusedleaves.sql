{{
  config(
    materialized = 'view',tags = ['eff_kpi','lastusedleaves']
    )
}}


WITH DISTINCT_SAP_USER_IDS AS (
	SELECT DISTINCT
		rls_region = CASE 
						WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = [grup/baskanlik_en]) = 'TR' THEN 'TUR'
						ELSE 'RUS' 
					 END
		,rls_group = CONCAT(
							UPPER((SELECT TOP 1 group_rls FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = [grup/baskanlik_en])),'_',
							CASE 
								WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = [grup/baskanlik_en]) = 'TR' THEN 'TUR'
								ELSE 'RUS' 
							END)
		,rls_company = CONCAT(UPPER(bordro_sirketi_kodu),'_',
							CASE 
								WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = [grup/baskanlik_en]) = 'TR' THEN 'TUR'
								ELSE 'RUS' 
							END)
		,rls_businessarea = CONCAT(externalcode_picklistoption,'_',
							CASE 
								WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = [grup/baskanlik_en]) = 'TR' THEN 'TUR'
								ELSE 'RUS' 
							END)
		,[sap_id]
		,[user_id]	
		,[global_id]
		,externalcode_picklistoption
		,bordro_sirketi_kodu
	FROM  {{ source('stg_hr_kpi', 'raw__hr_kpi_t_fact_sacreport') }}
)

SELECT
	d.rls_region
	,d.rls_group
	,d.rls_company
	,d.rls_businessarea
	,global_id = CAST(SFI.global_id AS NVARCHAR)
	,[user_id] = kisi_taniticisi
	,d.externalcode_picklistoption AS business_area
	,d.bordro_sirketi_kodu AS company_code
	,MAX(CAST(yazma_tarihi as DATE)) last_leave_date
FROM [aws_stage].[hr_kpi].[raw__hr_kpi_t_fact_timeaccountdetail] AS SFD
	LEFT OUTER JOIN [aws_stage].[hr_kpi].[raw__hr_kpi_t_fact_timeaccount] AS SFI ON SFI.[harici_kod]=SFD.[zaman_hesabi_harici_kod]
	LEFT JOIN DISTINCT_SAP_USER_IDS d ON d.user_id = SFI.kisi_taniticisi
WHERE 1=1
	AND yazma_turu ='EMPLOYEE_TIME'
	AND CAST(yazma_tarihi as DATE) <= GETDATE()
GROUP BY 
	d.rls_region
	,d.rls_group
	,d.rls_company
	,d.rls_businessarea
	,SFI.global_id
	,kisi_taniticisi
	,d.externalcode_picklistoption
	,d.bordro_sirketi_kodu