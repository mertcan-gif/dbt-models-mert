{{
  config(
    materialized = 'table',tags = ['hos_kpi']
    )
}}

select 
  [rls_region]
  ,[rls_group]
  ,[rls_company]
  ,[rls_businessarea]
  ,[company]
  ,[businessarea]
  ,[planned_cost_adjusment_flag]
  ,[operation_type_adjustment_helper]
  ,[maintenance_period_code]
  ,planned_date
  ,[operation_type]
  ,[operation_type_index]
  ,[planned_cost]
  ,[realized_cost]
  ,[plant]
  ,[plant_name]
  ,[technical_unit]
  ,[equipment_no]
  ,[equipment_description]
  ,[subequipment_no]
  ,[subequipment_description]
  ,[operation_description]
  ,[equipment_group]
  ,demarkization.[demarkation_category]
  ,[equipment_group_description]
  ,[maintenance_period]
  ,[maintenance_period_type]
  ,[order_opening_date]
  ,[order_closing_date]
  ,[equipment_type]
  ,[responsible_unit]
  ,[currency_planned_cost]
  ,[currency_realized_cost]
  ,[purchase_cost]
  ,[storage_cost]
  ,[labour_cost]
from {{ ref('stg__hos_kpi_t_fact_hostpitalmaintenance') }} fact
	LEFT JOIN {{ source('stg_sharepoint', 'raw__hos_kpi_t_fact_hospitaldemarkizationmatrix') }} demarkization on
	demarkization.[object_type] = fact.equipment_group