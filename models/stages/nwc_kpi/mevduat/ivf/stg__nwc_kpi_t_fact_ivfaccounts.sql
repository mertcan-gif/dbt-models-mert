{{
  config(
    materialized = 'table',tags = ['fi_kpi']
    )
}}



WITH Tally AS (
    -- Generate a series of numbers (days)
    SELECT TOP (DATEDIFF(DAY, (SELECT MIN(start_date) FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_ivffundbalances') }}), 
                          -- (SELECT MAX(end_date) FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_ivffundbalances') }})) + 1)
                          /** Snapshot kurgusu güncellenince aşağıdaki kısım kapatılıp yukarıdaki açılacak **/
                          (SELECT MAX(CAST(GETDATE() AS DATE)) FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_ivffundbalances') }})) + 1)
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
    FROM master.dbo.spt_values
)

SELECT 
    DATEADD(DAY, t.n, t_base.[start_date]) AS [date],  -- Generate all dates from [start_date] to [end_date]
    t_base.[company],
    t_base.[general_ledger_account],
    t_base.[bank_name],
    t_base.[is_foreign],
    t_base.[account_type],
    t_base.[deposit_demand_group],
    t_base.[amount_transaction_currency],
    t_base.[balance_ipb],
    t_base.[db_upload_timestamp],
    [end_date] = CAST(GETDATE() AS DATE)
 FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_ivffundbalances') }} t_base
CROSS APPLY (
    -- Generate dates only from [start_date] to [end_date]
    SELECT TOP (DATEDIFF(DAY, t_base.[start_date], CAST(GETDATE() AS DATE)) + 1) n 
    FROM Tally
) t
WHERE t_base.[start_date] IS NOT NULL 
  --AND t_base.[end_date] IS NOT NULL
  AND DATEADD(DAY, t.n, t_base.[start_date]) < CAST(GETDATE() AS DATE)  -- Ensure we don't exceed the end date
