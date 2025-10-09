{{
  config(
    materialized = 'table',tags = ['gyg_kpi']
    )
}}


SELECT
  year_int 
  ,month_int
  ,financial_center_code_adjusted as financial_center_code 
  ,commitment_item_code_adjusted as commitment_item_code
  ,amount_try = SUM(amount_try)
  ,amount_usd = SUM(amount_usd)
  ,amount_eur = SUM(amount_eur)
from {{ ref('stg__gyg_kpi_t_fact_realizedgygdetailed') }}
GROUP BY
  year_int 
  ,month_int
  ,financial_center_code_adjusted 
  ,commitment_item_code_adjusted 