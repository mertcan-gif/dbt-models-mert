{{
  config(
    materialized = 'table',tags = ['fi_kpi']
    )
}}
 

WITH cte_ivf AS (
  SELECT
    i.[company],
    [account_key] = CONCAT(i.company, i.account_type, i.deposit_demand_group, i.amount_transaction_currency, cast(i.start_date as date)),
    i.[bank_name],
    i.[is_foreign],
    i.[account_type],
    i.[deposit_demand_group],
    i.[amount_transaction_currency],
    i.[balance_ipb],
    i.[interest_rate],
    i.[db_upload_timestamp],
    i.[start_date],
    i.[end_date],
    i.[snapshot_date]
  FROM aws_stage.nwc_kpi.stg__nwc_kpi_t_fact_ivfbalancessnapshots i
 )

,pivotted_cte AS (

	SELECT
		*
		,ROW_NUMBER() OVER(PARTITION BY account_key ORDER BY start_date DESC,snapshot_date DESC) RN
  FROM cte_ivf
)

SELECT * FROM pivotted_cte
WHERE RN = 1
