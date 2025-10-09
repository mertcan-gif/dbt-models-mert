{{
  config(
    materialized = 'table',tags = ['rgy_kpi','rmore']
    )
}}

/* 
Date: 20250929
Creator: Elif Erdal
Report Owner: Beyzanur Gedikoğlu
Explanation:Bu sorguda  RGY şirketinin rmore ve power bi raporlarının kişi ve rapor özelinde görüntülenme sayıları
*/

with 
UNIONIZED_LOGS as (

  SELECT 
    [id]
    ,creation_time
    ,[user_id]
    ,[workspace_id] 
    ,[report_id]
    ,[report_type]
    ,[consumption_method]
    ,1 AS transaction_amount
    ,report_name
  FROM {{ ref('stg__powerbi_kpi_t_fact_viewreportlogs') }}
  WHERE report_name = 'RGY Dashboard'
 

  UNION ALL 

  SELECT 
      [id]
    ,creation_time
    ,[user_id]
    ,[workspace_id]
    ,[report_id]
    ,[report_type]
    ,[consumption_method]
    ,1 AS transaction_amount
    ,report_name
  FROM {{ ref('stg__rmore_kpi_t_fact_viewreportlogs') }}
  WHERE (report_name LIKE 'RGY%' 
  OR report_name LIKE '%IW%')

),
visit_count AS (
  SELECT
    l.*,
    -- o kişinin o rapora toplam kaç kez girdiği 
    COUNT(*) OVER (PARTITION BY l.user_id, l.report_name) AS visit_count_since_user_first,
    -- Son 30 günde giriş sayısı
    SUM(CASE WHEN l.creation_time >= DATEADD(DAY, -30, GETDATE()) THEN 1 ELSE 0 END)
            OVER (PARTITION BY l.user_id, l.report_name) AS visit_counts_last_30_days
  FROM UNIONIZED_LOGS l
)

SELECT 
  rls_region = CASE 
					WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_en) = 'TR' THEN 'TUR'
					ELSE 'RUS' 
				 END
	,rls_group = CONCAT(UPPER((SELECT TOP 1 group_rls FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_en)),'_',
						CASE 
							WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_en) = 'TR' THEN 'TUR'
							ELSE 'RUS' 
							END
						)
	,rls_company = CONCAT(UPPER(level_b.name_en),'_',
							CASE 
								WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_en) = 'TR' THEN 'TUR'
								ELSE 'RUS' 
							END
							)
	,rls_businessarea = CONCAT(UPPER(emp.business_area),'_',
								CASE 
									WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_en) = 'TR' THEN 'TUR'
									ELSE 'RUS' 
								END
								)
    ,v.[id]
    ,v.creation_time
    ,v.[user_id]
    ,CONCAT(emp.[name], ' ', emp.[surname]) AS name_surname
    ,v.[workspace_id]
    ,v.[report_id]
    ,v.[report_name]
    ,v.[report_type]
    ,v.[consumption_method]
    ,v.transaction_amount
    ,v.visit_count_since_user_first   -- o rapora bugüne kadar toplam kaç kez girmiş
    ,v.visit_counts_last_30_days    ---o rapora son otu gunde kac kez girmis
    ,fm.rgy_kpi
	  ,CASE 
			WHEN fm.rgy_kpi = 'TRUE' THEN 'authorized' ELSE 'unauthorized'
	END AS authorization_status
FROM visit_count v
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} emp ON emp.email_address = v.user_id
  LEFT JOIN {{ source('stg_rls', 'raw__rls_t_dim_functionalmatrixhol') }} fm ON fm.email = v.user_id AND fm.rgy_kpi = 'TRUE'
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_a') }} level_a ON level_a.code = emp.a_level_code
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_b') }} level_b ON level_b.code = emp.b_level_code
        WHERE emp.job_function NOT LIKE N'%VERİ ANALİTİĞİ%'
            AND emp.user_id NOT IN ('47055664', '47062222')
            
