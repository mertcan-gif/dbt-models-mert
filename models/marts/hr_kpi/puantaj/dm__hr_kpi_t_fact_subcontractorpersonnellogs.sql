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
  )

SELECT
	c.rls_region
	,c.rls_group
	,c.rls_company
	,rls_businessarea = CONCAT(m.werks , '_' , c.rls_region)
  ,[pernr] AS [sap_id]
  ,[projead] AS [project_name]
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
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} c ON c.RobiKisaKod = m.bukrs
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lf
  ON lf.lifnr = l.zztaseronno
LEFT JOIN (
            SELECT
              *
            FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hrp1010') }}
            WHERE  subty = '0001') hrp ON l.plans = hrp.objid
WHERE 1=1
      AND pernr <> '00000000'
      AND izindurum IN (N'ÇALIŞTI', N'BAYRAM ÇALIŞMASI', N'HAFTA TATİLİ ÇALIŞMASI')