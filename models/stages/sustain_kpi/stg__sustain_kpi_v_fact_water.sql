{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}
select 
	Yıl as year,
	ay as month,
	[Grup Şirket] as company,
	[Proje İşletme] as project,
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
	'Su' as fuel_type,
	cast(faaliyet_veri as float) as water_activity_value
from {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_4_1_2') }}
