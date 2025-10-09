{{
  config(
    materialized = 'incremental',tags = ['incremental_kpi']
    )
}}
 
SELECT 
  *
  ,snapshot_date = CAST(GETDATE() AS DATE)
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zhr_000_t_dwhlog') }}

{% if is_incremental() %}
    WHERE CONVERT(DATE, GETDATE()) > (
    SELECT MAX([snapshot_date]) FROM {{ this }}
)
{% endif %}