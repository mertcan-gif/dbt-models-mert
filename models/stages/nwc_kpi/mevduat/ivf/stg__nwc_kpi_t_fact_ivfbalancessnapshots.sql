{{
  config(
    materialized = 'incremental',tags = ['fi_kpi']
    )
}}
 
SELECT 
  *
  ,snapshot_date = CAST(GETDATE() AS DATE)
FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_ivffundbalances') }}

{% if is_incremental() %}
    WHERE CONVERT(DATE, GETDATE()) > (
    SELECT MAX([snapshot_date]) FROM {{ this }}
)
{% endif %}