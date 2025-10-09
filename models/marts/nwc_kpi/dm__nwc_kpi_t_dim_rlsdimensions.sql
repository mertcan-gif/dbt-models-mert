
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','rlsdimensions']
    )
}}

SELECT *
FROM {{ ref('stg__nwc_kpi_t_dim_rlsdimensions') }}
