{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}

WITH x as (
SELECT
year
FROM {{ ref('dm__sustain_kpi_t_fact_allscopes') }}
UNION ALL
SELECT
year
FROM {{ ref('dm__sustain_kpi_t_fact_energy') }}
UNION ALL
SELECT
year
FROM {{ ref('dm__sustain_kpi_t_fact_waste') }}
UNION ALL
SELECT
year
FROM {{ ref('dm__sustain_kpi_t_fact_water') }}
)
SELECT DISTINCT

   NULL AS [rls_region]
  ,NULL AS [rls_group]
  ,NULL AS [rls_company]
  ,NULL AS [rls_businessarea]
  ,year
FROM x
  where year is not null