{{
  config(
    materialized = 'incremental',tags = ['incremental_kpi']
    )
}}
 
SELECT 
  *
  ,snapshot_date = CAST(GETDATE() AS DATE)
FROM {{ ref('stg__hr_kpi_t_fact_activesapuserlist') }}
WHERE date = CAST(GETDATE() AS DATE)

{% if is_incremental() %}
    AND CONVERT(DATE, GETDATE()) > (
        SELECT MAX([snapshot_date]) FROM {{ this }}
    )
{% endif %}