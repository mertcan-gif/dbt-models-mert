{{
  config(
    materialized = 'table',tags = ['gyg_kpi']
    )
}}
WITH gyg_realized_revenue AS (
    SELECT 
        entity as fc_company,
        year_month,
        SUM(revenue) as revenue_try,
        SUM(revenue_ar_eur) as [revenue_eur]
    FROM {{ source('stg_fc_kpi', 'raw__fc_kpi_t_fact_fcdetails') }}
    GROUP BY 
        entity,
        year_month
)
SELECT *
FROM gyg_realized_revenue