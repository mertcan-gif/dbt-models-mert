{{
  config(
    materialized = 'table',tags = ['fi_kpi']
    )
}}

WITH Tally AS (
    -- Generate a series of numbers (days)
    SELECT TOP (DATEDIFF(DAY, (SELECT MIN([start_date]) FROM {{ ref('stg__nwc_kpi_t_fact_depositcockpit_rflow') }}), 
                          (SELECT MAX(end_date) FROM {{ ref('stg__nwc_kpi_t_fact_depositcockpit_rflow') }})) + 1)
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
    FROM master.dbo.spt_values
)

	SELECT 
		DATEADD(DAY, t.n, t_base.[start_date]) AS [date],  -- Generate all dates from [start_date] to [end_date]
		t_base.[company],
		t_base.account_number,--t_base.[general_ledger_account],
		t_base.[bank_name],
		t_base.[bank_country],
		t_base.[amount_transaction_currency],
		t_base.txt_balance,
		t_base.balance_usd,
		--t_base.[db_upload_timestamp],
		t_base.end_date,
		t_base.account_type
	FROM {{ ref('stg__nwc_kpi_t_fact_depositcockpit_rflow') }} t_base
	CROSS APPLY (
		-- Generate dates only from [start_date] to [end_date]
		SELECT TOP (DATEDIFF(DAY, t_base.[start_date], t_base.end_date) + 1) n 
		FROM Tally
	) t
	WHERE t_base.[start_date] IS NOT NULL 
	  AND t_base.end_date IS NOT NULL
	  AND DATEADD(DAY, t.n, t_base.[start_date]) < t_base.end_date  -- Ensure we don't exceed the end date