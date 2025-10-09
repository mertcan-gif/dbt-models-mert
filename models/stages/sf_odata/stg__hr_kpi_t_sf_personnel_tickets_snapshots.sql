{{
  config(
    materialized = 'incremental',tags = ['sf_odata']
    )
}}
 
SELECT
  [seq_number]
  ,[start_date]
  ,[user_id]
  ,[ticket_status_employee]
  ,[name]
  ,[surname]
  ,[ticket_status_position]
  ,[db_upload_timestamp]
  ,[snapshot_date] = GETDATE()
FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_personnel_tickets') }}


{% if is_incremental() %}
        WHERE CONVERT(DATE, GETDATE()) > (
            SELECT MAX([snapshot_date]) FROM {{ this }}
        )
{% endif %}