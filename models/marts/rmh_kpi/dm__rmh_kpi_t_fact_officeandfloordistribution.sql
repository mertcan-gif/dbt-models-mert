{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}

SELECT
	*
FROM {{ ref('stg__rmh_kpi_t_fact_officeandfloordistribution') }}