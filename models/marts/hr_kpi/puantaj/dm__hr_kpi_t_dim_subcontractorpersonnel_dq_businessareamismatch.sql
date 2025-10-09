{{
  config(
    materialized = 'table',tags = ['hr_kpi_puantaj']
    )
}}
WITH project_company_mapping AS (
  SELECT
    name1
    ,WERKS
    ,w.BWKEY
    ,bukrs
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} w
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} k ON w.bwkey = k.bwkey
  ),

keys_addition as (
SELECT
	c.rls_region
	,c.rls_group
	,c.rls_company
	,rls_businessarea = CONCAT(m.werks , '_' , c.rls_region)
  ,[pernr] AS [sap_id]
  ,t001w.name1 AS [project_name]
  ,[zztaseronno]
  ,[anadisiplin] AS [main_discipline]
  ,[altdisiplin] AS [sub_discipline]
  ,CASE
      WHEN hilfm = '001' THEN 'Beyaz Yaka'
      WHEN hilfm IN ('002', '003') THEN 'Mavi Yaka'
      ELSE hilfm
  END AS collar_type
  ,CAST(datum AS DATE) AS [working_date]
  ,[uzeit] AS [time]
  ,[plans]
  ,[izindurum] AS [leave_status]
  ,[uretimliuretimsiz] AS [productive_nonproductive]
  ,[gecegunduz] AS [night_day]
  ,[zzdurum]
  ,[personeltc]
  ,[zzcalismasaati] AS [zzworking_hours]
  ,[taseronaciklama] AS [subcontractor_description]
  ,[zzfazlamesai] AS [zzovertime]
  ,[zzsahamsaatgrs] AS [zzfield_m_hour_grs]
  ,[ronesansaciklama] AS [ronesans_description]
  ,[zzmeyersaat] AS [zzmeyer_hours]
  ,[zzikgrs] AS [zzhr_entry]
  ,[ikaciklama] AS [hr_description]
  ,[fkber] AS [fkber]
  ,[islemtarihi] AS [transaction_date]
  ,[islemsaati] AS [transaction_hours]
  ,l.[werks] AS [business_area]
  ,[zekipno] AS [zteamno]
  ,lf.[name1] AS company_name
  ,l.[db_upload_timestamp]
  ,c.KyribaGrup as [group]
  ,m.bukrs as company
  ,m.name1 as name 
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_010_t_cs_log') }} l
LEFT JOIN project_company_mapping m ON l.werks= m.werks
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w on t001w.werks = m.werks
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} c ON c.RobiKisaKod = m.bukrs
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lf ON lf.lifnr = l.zztaseronno
LEFT JOIN (
            SELECT
              *
            FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hrp1010') }}
            WHERE  subty = '0001') hrp ON l.plans = hrp.objid
WHERE 1=1
      AND pernr <> '00000000'
      AND izindurum IN (N'ÇALIŞTI', N'BAYRAM ÇALIŞMASI', N'HAFTA TATİLİ ÇALIŞMASI')
),
final as 
(select 
  rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
  ,[rls_region]
  ,[rls_group]
  ,[rls_company]
  ,[rls_businessarea]
  ,[sap_id]
  ,[project_name]
  ,[zztaseronno]
  ,[main_discipline]
  ,[sub_discipline]
  ,[collar_type]
  ,[working_date]
  ,[time]
  ,[plans]
  ,[leave_status]
  ,[productive_nonproductive]
  ,[night_day]
  ,[zzdurum]
  ,[personeltc]
  ,[zzworking_hours]
  ,[subcontractor_description]
  ,[zzovertime]
  ,[zzfield_m_hour_grs]
  ,[ronesans_description]
  ,[zzmeyer_hours]
  ,[zzhr_entry]
  ,[hr_description]
  ,[fkber]
  ,[transaction_date]
  ,[transaction_hours]
  ,[business_area]
  ,[zteamno]
  ,[company_name]
  ,[db_upload_timestamp]
  ,[group]
  ,[company]
  ,[name]
  ,[join_key]=CONCAT([working_date], '_', sap_id)
from keys_addition)

select 
 fact.rls_key
,fact.[rls_region]
,fact.[rls_group]
,fact.[rls_company]
,fact.[rls_businessarea]
,fact.[sap_id]
,fact.[project_name]
,fact.[zztaseronno]
,fact.[main_discipline]
,fact.[sub_discipline]
,fact.[collar_type]
,fact.[working_date]
,fact.[time]
,fact.[plans]
,fact.[leave_status]
,fact.[productive_nonproductive]
,fact.[night_day]
,fact.[zzdurum]
,fact.[personeltc]
,fact.[zzworking_hours]
,fact.[subcontractor_description]
,fact.[zzovertime]
,fact.[zzfield_m_hour_grs]
,fact.[ronesans_description]
,fact.[zzmeyer_hours]
,fact.[zzhr_entry]
,fact.[hr_description]
,fact.[fkber]
,fact.[transaction_date]
,fact.[transaction_hours]
,fact.[business_area]
,fact.[zteamno]
,fact.[company_name]
,fact.[db_upload_timestamp]
,fact.[group]
,fact.[company]
,fact.[name]
,fact.[join_key]
,dim.[join_key] as 'dim_join_key'
,dim.[position]
,dim.[statu]
,dim.[direct_indirect]
,dim.[blue_white_collar]
,dim.[project]
,dim.[production_class]
,dim.[team_code]
,dim.[team_based]
,CASE WHEN dim.[transportation] = '' THEN N'Kendi İmkanlarıyla' ELSE dim.[transportation] END AS [transportation]
,CASE WHEN dim.[accommodation] = '' THEN N'Kendi İmkanlarıyla' ELSE dim.[accommodation] END AS [accommodation]
,dim.[company_class]
,dim.[sub_subcontractor]
,dim.[subcontractor]
,dim.[employee_group]
,dim.[task_type]
,dim.[location]
,dim_business_area=dim.[business_area]
from final as fact 
left outer join {{ ref('dm__hr_kpi_t_dim_subcontractorpersonnel_transformed') }} as dim on fact.join_key=dim.join_key
where 1=1
and dim.join_key is not null 
and working_date>='2024-01-01'
and fact.[business_area]<>dim.[business_area]