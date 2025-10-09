
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','tahakkuk']
    )
}}

SELECT *
  ,business_area_concatted = CONCAT(business_area,' - ',business_area_description)
FROM {{ ref('stg__nwc_kpi_t_fact_costrealizationadjusted') }}
