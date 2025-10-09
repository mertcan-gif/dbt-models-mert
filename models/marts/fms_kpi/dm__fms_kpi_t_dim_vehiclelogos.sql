{{
  config(
    materialized = 'table',tags = ['fms_kpi']
    )
}}

SELECT
    brand_name
    ,brand_logo
FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_dim_vehiclelogos') }}