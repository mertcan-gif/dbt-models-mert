{{
  config(
    materialized = 'table',tags = ['fi_kpi']
    )
}}

WITH Tally AS (
    -- Generate a series of numbers (days)
    SELECT TOP (DATEDIFF(DAY, (SELECT MIN([date]) FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_yuvambankbalances') }}), 
                          (SELECT MAX([yuvam_end_date]) FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_yuvambankbalances') }})) + 1)
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
    FROM master.dbo.spt_values
)

SELECT 
    DATEADD(DAY, t.n, t_base.[date]) AS [date],  -- Generate all dates from [date] to [yuvam_end_date]
    t_base.[company],
    t_base.[general_ledger_account],
    t_base.[bank_name],
    t_base.[is_foreign],
    t_base.[amount_transaction_currency],
    t_base.[balance_ipb],
    t_base.[db_upload_timestamp],
    t_base.[yuvam_end_date]
FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_yuvambankbalances') }} t_base
CROSS APPLY (
    -- Generate dates only from [date] to [yuvam_end_date]
    SELECT TOP (DATEDIFF(DAY, t_base.[date], t_base.[yuvam_end_date]) + 1) n 
    FROM Tally
) t
WHERE t_base.[date] IS NOT NULL 
  AND t_base.[yuvam_end_date] IS NOT NULL
  AND DATEADD(DAY, t.n, t_base.[date]) < t_base.[yuvam_end_date]  -- Ensure we don't exceed the end date
