{{
  config(
    materialized = 'view',tags = ['to_kpi']
    )
}}
/*
Bu View'da, bir önceki hafta verisi DB'ye basılmamış olan projeler tespit edilir.
ED'da verisi basılmamış bu haftaların verilerinin boş gözükmemesi için, 
bir önceki haftanın verileri de bu haftanın verileri olarak gösterilir 
*/
WITH sorted_cte AS(
/*
Bu cte All tablosunun (project_id bazında),
reporting_date ve data_entry_timestamp'e göre en güncel verilerini gösterir (ROW_NUM = 1)
*/
	SELECT 
		*
		,ROW_NUMBER() OVER(PARTITION BY project_id ORDER BY reporting_date DESC, data_entry_timestamp DESC) AS ROW_NUM
	FROM {{source('stg_to_kpi','raw__to_kpi_t_dim_all')}}  
),

projects_with_current_report AS (
/*
Bu cte All tablosunda veri eksiği bulunmayan project_id'leri gösterir
*/
	SELECT DISTINCT project_id FROM sorted_cte
	WHERE 1=1
		AND ROW_NUM = 1
		AND DATEPART(WEEKDAY, reporting_date) = 6
		AND reporting_date < GETDATE()
		AND reporting_date > DATEADD(WEEK,-1,GETDATE())
),

projects_without_current_report AS (
/*
Bu cte All tablosunda, bir önceki hafta verisi basılmamış olan project_id'leri gösterir
*/
	SELECT 
		DISTINCT project_id 
	FROM {{source('stg_to_kpi','raw__to_kpi_t_dim_all')}}
	WHERE 1=1
		AND project_id NOT IN (SELECT * FROM projects_with_current_report)
)

/*
Bu sorguda, son hafta güncel verisi bulunmayan projenin verileri,
en son DB'ye yüklenmiş olan verileri gösterecek şekilde güncellenir.
*/

SELECT
	[project_id],
	[data_entry_timestamp] = GETDATE(),
	[reporting_date] = DATEADD(DAY,7,[reporting_date]),
	[db_upload_timestamp]  = GETDATE(),
	[employer],
	[contract_amount],
	[project_commencement_date],
	[project_finish_date],
	[gba],
	[baseline_revision_date],
	[completion_accrual_contract_amount],
	[invoiced_progress_payment_amount_ipd_evat],
	[invoice_pending_progress_payment],
	[interim_payment_progress_epd],
	[interim_payment_progress_based_on_contract],
	[cash_recieved],
	[at_completion_cash],
	[overdue_receivables],
	[remaining_advance_payment_amount],
	[advance_payment_guarantee_letter_amount],
	[advance_payment_guarantee_letter_end_date],
	[performance_guarantee_letter_amount],
	[performance_guarantee_letter_end_date],
	[site_delivery_date],
	[contract_finish_date],
	[target_finish_date],
	[contract_start_date],
	[time_progress],
	[project_duration],
	[progress_payment_notes],
	[one_month_look_ahead],
	[critical_path],
	[total_bid_packages],
	[planned_bid_packages],
	[completed_bid_packages],
	[critical_notes],
	[subcontractor_notes],
	[indirect_personnel],
	[total_advance_payment_amount],
	[progress_payment_change_order],
	[price_difference_at_progress_payment],
	[total_recievables]
FROM sorted_cte
WHERE 1=1
	AND project_id IN (SELECT * FROM projects_without_current_report)
	AND ROW_NUM = 1



