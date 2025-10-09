{{ config( materialized = 'table', tags = ['metadata_kpi'] ) }}

select user_id as email from {{ ref('stg__rnet_kpi_t_fact_licenses') }}
UNION 
select user_id from {{ ref('stg__rnet_kpi_t_fact_viewreportlogs') }}
UNION
select user_id from {{ ref('stg__powerbi_kpi_t_fact_viewreportlogs') }}
UNION
select email_address from {{ ref('stg__powerbi_kpi_t_fact_licenses') }}
UNION 
select user_id from {{ ref('stg__s4_kpi_t_fact_viewreportlogs') }}