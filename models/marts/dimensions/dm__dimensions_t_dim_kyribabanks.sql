{{
  config(
    materialized = 'table',tags = ['banks'],grants = {'select': ['rflow_user']}
    )
}}


SELECT 
		*
from {{ source('stg_fi_kpi', 'raw__fi_kpi_t_dim_kyribabanks') }}
where 1=1
	