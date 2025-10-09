{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

WITH unpivot_data AS (
SELECT 
    [company],
    [business_area],
    [project_name],
	[report_date],
    data_control_date,
    [attribute],
    [value]
FROM 
    (SELECT 
         [company],
         [business_area],
         [project_name],
		 [report_date],
         data_control_date,
         CAST([number_of_employees] AS NVARCHAR) AS [number_of_employees],
         CAST([man_hour] AS NVARCHAR) AS [man_hour],
         CAST([lost_non_time_bound_man_hour] AS NVARCHAR) AS [lost_non_time_bound_man_hour],
         CAST([fatal_accident_hour] AS NVARCHAR) AS [fatal_accident_hour],
         CAST([lost_time_accident_count] AS NVARCHAR) AS [lost_time_accident_count],
         CAST([restricted_work_accident_count] AS NVARCHAR) AS [restricted_work_accident_count],
         CAST([first_aid_treated_accident_count] AS NVARCHAR) AS [first_aid_treated_accident_count],
         CAST([medically_treated_accident_count] AS NVARCHAR) AS [medically_treated_accident_count],
         CAST([property_damage_accident_count] AS NVARCHAR) AS [property_damage_accident_count],
         CAST([traffic_accident_count] AS NVARCHAR) AS [traffic_accident_count],
         CAST([total_recordable_accident_count] AS NVARCHAR) AS [total_recordable_accident_count],
         CAST([total_unsafe_behavior_count] AS NVARCHAR) AS [total_unsafe_behavior_count],
         CAST([hypo_major_count] AS NVARCHAR) AS [hypo_major_count],
         CAST([km_driven] AS NVARCHAR) AS [km_driven],
         CAST([lost_time_accident_frequency_rate_lti] AS NVARCHAR) AS [lost_time_accident_frequency_rate_lti],
         CAST([total_recordable_accident_rate] AS NVARCHAR) AS [total_recordable_accident_rate],
         CAST([hypo_major_frequency_rate] AS NVARCHAR) AS [hypo_major_frequency_rate],
         CAST([mvi_rate] AS NVARCHAR) AS [mvi_rate],
         CAST([lost_time_accident_frequency_rate_target] AS NVARCHAR) AS [lost_time_accident_frequency_rate_target],
         CAST([total_recordable_accident_frequency_rate_target] AS NVARCHAR) AS [total_recordable_accident_frequency_rate_target]
     FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_isgc') }}) AS raw_data
UNPIVOT
    (
        [value] FOR [attribute] IN 
        ([number_of_employees],
         [man_hour],
         [lost_non_time_bound_man_hour],
         [fatal_accident_hour],
         [lost_time_accident_count],
         [restricted_work_accident_count],
         [first_aid_treated_accident_count],
         [medically_treated_accident_count],
         [property_damage_accident_count],
         [traffic_accident_count],
         [total_recordable_accident_count],
         [total_unsafe_behavior_count],
         [hypo_major_count],
         [km_driven],
         [lost_time_accident_frequency_rate_lti],
         [total_recordable_accident_rate],
         [hypo_major_frequency_rate],
         [mvi_rate],
         [lost_time_accident_frequency_rate_target],
         [total_recordable_accident_frequency_rate_target])
    ) AS UnpivotedTable
	)

SELECT 
	rls_region = cm.RegionCode
	,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
	,rls_company = company + '_' + cm.RegionCode 
	,rls_businessarea = business_area + '_' + cm.RegionCode
    ,[group] = cm.KyribaGrup
	,company
	,business_area
	,t.name1 as businessarea_name
	,attribute
	,CAST(value as float) value
	,CAST(report_date AS date) report_date
    ,CAST(data_control_date AS date) AS data_control_date
FROM unpivot_data ud
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm ON cm.KyribaKisaKod = ud.company
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t ON TRIM(ud.business_area) = t.werks