
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','projectprogress']
    )
}}

SELECT *
FROM {{ ref('stg__nwc_kpi_t_fact_projectprogress') }}
