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
  ,[company]
  ,[business_area]
  ,t.name1 as businessarea_name
  ,[budget_period]
  ,CAST([total_earth_movement] AS float) total_earth_movement
  ,CAST([total_concrete_volume_m3] AS float) total_concrete_volume_m3
  ,CAST([total_iron_quantity_ton] AS float) total_iron_quantity_ton
  ,CAST([total_steel_rail_quantity_ton] AS float) total_steel_rail_quantity_ton
  ,CAST([total_bitumen_quantity_ton] AS float) total_bitumen_quantity_ton 
  ,CAST([total_diesel_quantity_liters] AS float) total_diesel_quantity_liters
  ,CAST([total_number_of_railswitch] AS float) total_number_of_railswitch
  ,CAST([date] as date) date
  ,CAST([Data Control Date] AS DATE) AS data_control_date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_generalsizeinformation') }} gsi
    LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on TRIM(gsi.company) = cm.KyribaKisaKod
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t ON TRIM(gsi.business_area) = t.werks
)

SELECT	
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
