{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}

select 
'Birim' as seviye,
dp.*
from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_department') }} dp 
left join {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_positionorgchartobject') }} opo on opo.incumbent_sfid = dp.gorevdeki_kisi_sf_id
where 1=1 
and pstn_entity_parent_code is null
union all
select
'Bölüm/Proje/Isletme' as seviye,
dp.*
from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_subdivision') }} dp 
left join {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_positionorgchartobject') }} opo on opo.incumbent_sfid = dp.gorevdeki_kisi_sf_id
where 1=1 
and dp.pstn_entity_parent_code is null
union all
select
	   'Bölge/fonksiyon/BU' as seviye
	  , dp.[durum]
      ,dp.[sirket_grubu]
      ,dp.[entity_code]
      ,dp.[entity_name]
      ,dp.[pstn_entity_parent_code]
      ,dp.[pstn_entity_parent_name]
      ,dp.[org_entity_samelevel_code]
      ,dp.[org_entity_samelevel_name]
      ,dp.[head_of_unit_position]
      ,dp.[unvan]
      ,dp.[gorevdeki_kisi_sf_id]
      ,dp.[gorevdeki_kisi_adi]
      ,dp.[son_degisiklik_tarihi]
      ,dp.[db_upload_timestamp]
from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_division') }} dp 
left join {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_positionorgchartobject') }} opo on opo.incumbent_sfid = dp.gorevdeki_kisi_sf_id
where 1=1 
and dp.pstn_entity_parent_code is null
union all
select
	  'Sirket' as seviye
	  , dp.[durum]
      ,dp.[sirket_grubu]
      ,dp.[entity_code]
      ,dp.[entity_name]
      ,dp.[pstn_entity_parent_code]
      ,dp.[pstn_entity_parent_name]
      ,dp.[org_entity_samelevel_code]
      ,cast(dp.org_entity_samelevel_name as varchar(max))
      ,dp.[head_of_unit_position]
      ,dp.[unvan]
      ,dp.[gorevdeki_kisi_sf_id]
      ,dp.[gorevdeki_kisi_adi]
      ,dp.[son_degisiklik_tarihi]
      ,dp.[db_upload_timestamp]
from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_businessunit') }} dp 
left join {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_positionorgchartobject') }} opo on opo.incumbent_sfid = dp.gorevdeki_kisi_sf_id
where 1=1 
and dp.pstn_entity_parent_code is null