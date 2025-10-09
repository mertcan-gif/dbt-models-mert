{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

WITH _raw AS (
SELECT 
  rls_region = cm.RegionCode
  ,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
  ,rls_company = cm.KyribaKisaKod + '_' + cm.RegionCode
  ,rls_businessarea = TRIM(business_area) + '_' + cm.RegionCode
  ,cm.KyribaGrup as [group]
  ,[part]
  ,priority as part_order
  ,[company]
  ,[business_area]
  ,t.name1 as businessarea_name
  ,[main_discipline]
  ,[structure_type]
  ,[sub_discipline]
  ,CAST([weight] AS float) weight
  ,COALESCE(TRY_CAST([total_amount] AS FLOAT), NULL) total_amount 
  ,[quantity_unit] 
  ,COALESCE(TRY_CAST([planned_amount_until_report_date] AS FLOAT), 0) [planned_amount_until_report_date] 
  ,TRY_CAST([completed_amount] AS float) [completed_amount]
  ,TRY_CAST([remaining_amount] AS float) [remaining_amount]
  ,CAST([total_man_hour] AS float) [total_man_hour]
  ,CAST([planned_man_hour_until_report_date] AS float) [planned_man_hour_until_report_date]
  ,CAST([earned_man_hours] AS float) [earned_man_hours]
  ,[spent_man_hours]
  ,CAST([remaining_man_hour] AS float) [remaining_man_hour]
  ,CAST([total_planned_equivalent_hour] AS float) [total_planned_equivalent_hour]
  ,CAST([planned_equivalent_hour_until_report_date] AS float) [planned_equivalent_hour_until_report_date]
  ,CAST([earned_equivalent_hour] AS float) [earned_equivalent_hour]
  ,CAST([planned_physical_progress_until_report_date] AS float) [planned_physical_progress_until_report_date] 
  ,CAST([physical_progress] AS float) [physical_progress]
  ,CAST([date] AS date) date
  ,CAST(data_control_date AS date) AS data_control_date
FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_overallprogressstatus') }} ps
  LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on TRIM(ps.company) = cm.KyribaKisaKod
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t ON TRIM(ps.business_area) = t.werks
)

SELECT	
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
