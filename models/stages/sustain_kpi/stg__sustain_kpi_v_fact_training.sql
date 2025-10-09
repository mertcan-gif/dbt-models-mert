{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}

select
 * 
from  {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_training') }}
