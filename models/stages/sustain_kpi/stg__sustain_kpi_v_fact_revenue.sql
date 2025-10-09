{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}

select
[sirket] as company
,[yil] as year
,[ciro] as revenue
,[db_upload_timestamp]
,[Ay] as month
from 
{{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_revenue') }}