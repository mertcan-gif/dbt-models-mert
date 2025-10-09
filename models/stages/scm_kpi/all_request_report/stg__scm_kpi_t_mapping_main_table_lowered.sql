{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}

SELECT DISTINCT
    eban.zzariba_header_txt,
    eban.zzariba_req_no,
    eban.werks,
    eban.ekgrp,
    eban.ZZBTP_USER,
    eb.bnfpo,
    eb.badat,
    k.process_date,
    t.max_process_date,
    t024.eknam
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eban') }} eban
    LEFT JOIN (
        SELECT
            zzariba_req_no,
            ZZBTP_USER,
            COUNT(DISTINCT CAST(bnfpo AS FLOAT)/10) as bnfpo,
            MAX(TRY_CAST(BADAT AS DATETIME)) AS badat
        FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eban') }}
        WHERE 1=1  
        GROUP BY zzariba_req_no, ZZBTP_USER
    ) as eb ON eb.zzariba_req_no = eban.zzariba_req_no
    LEFT JOIN (
        SELECT id, process_date
        FROM {{ ref('stg__scm_kpi_t_fact_processingtimes') }}
        WHERE tdf_approval_group = 'Talep Yaratilma'
    ) k ON k.[id] = eban.zzariba_req_no
    LEFT JOIN (
        SELECT id, MAX(process_date) as max_process_date
        FROM {{ ref('stg__scm_kpi_t_fact_processingtimes') }}
        WHERE process_category = 'Talep'  
        GROUP BY id
    ) t ON t.[id] = eban.zzariba_req_no
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t024') }} t024 ON t024.ekgrp = eban.ekgrp
WHERE eban.zzariba_req_no IS NOT NULL AND eban.zzariba_req_no <> ''