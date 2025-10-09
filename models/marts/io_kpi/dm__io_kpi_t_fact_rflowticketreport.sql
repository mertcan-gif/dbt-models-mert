{{
  config(
    materialized = 'table',tags = ['io_kpi']
    )
}}

with final as (
    SELECT 
        rls_region,
        rls_group,
        rls_company,
        rls_businessarea = CONCAT('_', rls_region),
        t1.ticketno AS ticket_number,
        t1.durum_tanim AS ticket_status_description,
        t1.istektip AS ticket_request_type,
        t1.istektip_tanim AS ticket_request_description,
        t1.istekaltip AS ticket_sub_request_type,
        t1.istekaltip_tanim AS ticket_sub_request_description,
        t3.istektip AS support_request_type,
        t3.istektip_tanim AS support_request_description,
        t3.istekaltip AS support_sub_request_type,
        t3.istekaltip_tanim AS support_sub_request_description,
        t1.kategori AS category,
        t2.statu_tanim AS task_status_description,
        TRY_CAST(CONCAT(t1.tarih, ' ', t1.saat) AS datetime) AS creation_date,
        DATEDIFF(MINUTE, 
            TRY_CAST(CONCAT(t1.tarih, ' ', t1.saat) AS datetime),
            CASE 
                WHEN t2.statu_tanim LIKE N'%Task oluştu%' THEN TRY_CAST(CONCAT(t2.tarih, ' ', t2.saat) AS datetime)
                ELSE NULL 
            END
        ) AS task_conversion_time,
        DATEDIFF(MINUTE, 
            TRY_CAST(CONCAT(t1.tarih, ' ', t1.saat) AS datetime),
            CASE 
                WHEN t2.statu_tanim LIKE N'%Tamamlandı%' THEN TRY_CAST(CONCAT(t2.tarih, ' ', t2.saat) AS datetime)
                ELSE NULL 
            END
        ) AS completion_time,
        TRY_CAST(t7.efor AS float) AS effort,
        t57.kaynaktip AS source_type,
        t3.taskno AS task_number,
        TRY_CAST(CONCAT(t3.tarih, ' ', t3.saat) AS datetime) AS task_creation_date,
        t3.hizmettipi AS service_type,
        t7.danisman AS specialist,
        t7.adsoyad AS full_name,
        TRY_CAST(t3.tahbas AS datetime) AS estimated_start_date,
        TRY_CAST(t3.tahbitis AS datetime) AS estimated_end_date,
        DATEDIFF(MINUTE, TRY_CAST(t3.tahbas AS datetime), TRY_CAST(t3.tahbitis AS datetime)) AS estimated_date_diff,
        TRY_CAST(t7.tarih AS datetime) AS due_date,
        TRY_CAST(t1.ongorefor AS FLOAT) AS estimated_effort,
        TRY_CAST(t7.toplamefor AS FLOAT) AS total_effort,
        t1.baslik AS title,
        t1.per_alankod AS company,
        t1.departmankod AS department,
        t70.sicil AS personnel_id,
        t58.yetki AS [authorization]
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zitp_t_01') }} t1
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zitp_t_02') }} t2 ON t1.ticketno = t2.ticketno
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zitp_t_03') }} t3 ON t1.ticketno = t3.ticketno
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zitp_t_07') }} t7 ON t3.taskno = t7.taskno
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zitp_t_57') }} t57 ON t7.danisman = t57.sicil
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zitp_t_70') }} t70 ON t7.danisman = t70.sicil and t1.istektip = t70.istektip and t1.istekaltip = t70.istekaltip
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zitp_t_58') }} t58 ON t7.danisman = t58.sicil
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON t1.per_alankod = dim_comp.RobiKisaKod
WHERE t2.statu_tanim like N'%Task oluştu%' or t2.statu_tanim like N'%Tamamlandı%'
)

select 
	rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
	,* 
from final
