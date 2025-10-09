{{
  config(
    materialized = 'table',tags = ['nwc_kpi','dimensions']
    )
}}


SELECT 
	*
FROM {{ ref('stg__dimensions_t_dim_dailys4currencies') }}