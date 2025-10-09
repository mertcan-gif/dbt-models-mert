{{
  config(
    materialized = 'table',tags = ['io_kpi']
    )
}}

with tickets as (
    SELECT
        [ticketno]
        ,[ticket]
        ,[olusturansicil]
        ,[olusturanadsoyad]
        ,[tarih]
        ,[saat]
        ,opening_time = CAST(CONCAT([tarih], ' ', [saat]) AS datetime)
        ,t_01.[istektip]
        ,[istektip_tanim]
        ,[istekaltip]
        ,[istekaltip_tanim]
        ,[durum]
        ,[durum_tanim]
        ,[statu]
        ,[statu_tanim]
        ,[baslik]
        ,[aciklama]
        ,[kapattarih]
        ,[kapatsaat]
        ,closing_time =
            case
                when kapattarih <> '0000-00-00' THEN CAST(CONCAT([kapattarih], ' ', [kapatsaat]) AS datetime)
                when kapattarih = '0000-00-00' THEN CAST(GETDATE() AS datetime)
            END
        ,[btnotu]
        ,[kategori_tanim]
        ,[yerinead]
        ,[yerinesicil]
        ,[taskno]
        ,[ongorefor]
        ,[ongortermin]
        ,[taskcevir]
        ,[oncelik]
        ,[oncelik_tanim]
        ,[per_alankod]
        ,[per_alan]
        ,[peraltalankod]
        ,[peraltalan]
        ,[perorgalan]
        ,[masrafyeri]
        ,[departmankod]
        ,[departman]
        ,[pozisyonkod]
        ,[pozisyon]
        ,[sicil]
        ,[yonsicil]
        ,[yonadsoyad]
        ,[isalan]
        ,[isalankod]
        ,[maviyakad]
        ,[maviyakadepartman]
        ,[maviyakaeposta]
        ,[maviyakaproje]
        ,[maviyakaprojead]
        ,[maviyakapozsiyon]
        ,[maviyakasicil]
        ,[sorumlusicil]
        ,[sorumluad]
        ,[sirketid]
        ,category.kategori1
        ,category.kategori2
        ,category.kategori3
    FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zitp_t_01') }} as t_01
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zitp_t_52') }} AS category 
        on category.istektip=t_01.istektip and category.istekalttip=t_01.istekaltip
)

,task AS (
    SELECT
        [taskno]
        ,[aktiviteno]
        ,[danisman]
        ,t07_tasks.[adsoyad]
        ,[tarih]
        ,[aciklama]
        ,[toplam_efor] = CAST([efor] AS decimal(10,2))
        ,[ice_for] = CAST([icefor] AS decimal(10,2))
        ,[fatura_efor] = CAST([faturaefor] AS decimal(10,2))
        ,t07_tasks.[kaynaktip]
        ,t57_danisman.firma
        ,t58_usertype.yetki
    FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zitp_t_07') }} as t07_tasks
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zitp_t_57') }} as t57_danisman
        on t57_danisman.sicil=t07_tasks.danisman
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zitp_t_58') }} as t58_usertype ON t58_usertype.sicil= t07_tasks.danisman
)

,tasks_grouped as (
    SELECT
        taskno
        ,responsible_internal = STRING_AGG(CASE WHEN kaynaktip = 'I' THEN danisman END, '-')
        ,responsible_internal_name = STRING_AGG(CASE WHEN kaynaktip = 'I' THEN adsoyad END, '-')
        ,responsible_external = STRING_AGG(CASE WHEN kaynaktip = 'O' THEN danisman END, '-')
        ,responsible_external_name = STRING_AGG(CASE WHEN kaynaktip = 'O' THEN adsoyad END, '-')
        ,tasks_total_activities = COUNT(DISTINCT(aktiviteno))
        ,tasks_unique_peoples = COUNT(DISTINCT(danisman))
        ,total_effort= SUM([toplam_efor])
        ,internal_total_effort = SUM(CASE WHEN kaynaktip = 'I' THEN toplam_efor ELSE 0 END)
        ,internal_total_effort_ams = SUM(CASE WHEN kaynaktip = 'I' AND yetki='AMSUSER' THEN toplam_efor ELSE 0 END)
        ,external_total_effort = SUM(CASE WHEN kaynaktip = 'O' THEN toplam_efor ELSE 0 END)
        ,external_total_effort_ams = SUM(CASE WHEN kaynaktip = 'O' AND yetki='AMSUSER' THEN toplam_efor ELSE 0 END)
        ,realized_total_effort = SUM(CASE WHEN kaynaktip = 'O' THEN toplam_efor ELSE 0 END)
    FROM task
    GROUP BY taskno
)

select
    rls_region
	,rls_group
	,rls_company
	,rls_businessarea = CONCAT('_', rls_region)
    ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
    ,[ticketno]
    ,[olusturansicil]
    ,[olusturanadsoyad]
    ,[tarih]
    ,[saat]
    ,opening_time
    ,[kapattarih]
    ,[kapatsaat]
    ,closing_time
    ,sla_minutes = datediff(minute, opening_time, closing_time)
    ,sla_days = cast(cast(datediff(minute, opening_time, closing_time) as float)/1440 as decimal(10,2))
    ,[istektip_tanim]
    ,[istekaltip_tanim]
    ,[durum_tanim]
    ,[statu_tanim]
    ,[baslik]
    ,[kategori_tanim]
    ,[yerinead]
    ,[yerinesicil]
    ,tickets.[taskno]
    ,[ongorefor]
    ,[ongortermin]
    ,[oncelik_tanim]
    ,[per_alankod]
    ,[per_alan]
    ,[peraltalan]
    ,[perorgalan]
    ,[masrafyeri]
    ,[departmankod]
    ,[departman]
    ,[pozisyonkod]
    ,[pozisyon]
    ,[sicil]
    ,[yonsicil]
    ,[yonadsoyad]
    ,[isalan]
    ,[isalankod]
--  ,[maviyakad]
--  ,[maviyakadepartman]
--  ,[maviyakaeposta]
--  ,[maviyakaproje]
--  ,[maviyakaprojead]
--  ,[maviyakapozsiyon]
--  ,[maviyakasicil]
    ,[sorumlusicil]
    ,[sorumluad]
    ,[sirketid]
    ,responsible_internal
    ,responsible_internal_name
    ,responsible_external
    ,responsible_external_name
    ,tasks_total_activities
    ,tasks_unique_peoples
    ,total_effort
    ,internal_total_effort
    ,internal_total_effort_ams
    ,external_total_effort
    ,external_total_effort_ams
    ,realized_total_effort
    ,tickets.aciklama
    ,main_category = tickets.kategori1
    ,sub_category = tickets.kategori2
    ,detailed_category = tickets.kategori3
from tickets
    left join tasks_grouped on tasks_grouped.taskno=tickets.taskno
    LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON tickets.per_alankod = dim_comp.RobiKisaKod