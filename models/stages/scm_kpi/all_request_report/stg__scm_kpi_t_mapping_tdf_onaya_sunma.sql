{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}

WITH tdf_onaya_sunma AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY tdf_id ORDER BY process_date DESC, process_time DESC) as rn_1,
            CAST(process_date AS DATETIME) + CAST(process_time AS DATETIME) as tdf_onaya_sunma_tarih
        FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_wf_log') }}
        WHERE 1=1
            AND process_id = ''
    ) subqry
    WHERE subqry.rn_1 = 1
)

select *
from tdf_onaya_sunma