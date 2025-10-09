{{
  config(
    materialized = 'table',tags = ['rmg_kpi']
    )
}}

WITH main_cte AS (
SELECT
	[YIL-AY] AS year_month,
	Sirket AS company,
	Proje AS business_area,
	[HAKEDİŞ Tanımı] AS [definition],
	[Teknik Ofis Personeli] AS personnel,
	[Teknik Ofis/Kısım Şefi] AS section_chief,
	[ŞANTİYE] AS worksite,
	[İşletme Senaryosu] AS business_scenario,
	[İşletme Senaryosu2] AS business_scenario_2,
	[Toplam Hakediş TL] AS total_progress_payment,
	TRY_CAST([Hakediş Yayını] AS date) AS progress_payment_release,
	TRY_CAST([Mutabakat Yayını] AS date) AS reconciliation_release,
	TRY_CAST([RCT Onay/Kontrol] AS date) AS rct_approval_control,
	TRY_CAST([REC SAS Dönüş] AS date) AS rec_sas_return,
	TRY_CAST([SD Kaydı] AS date) AS sd_entry_record,
	TRY_CAST([Fatura Kesim] AS date) AS invoice_issuance
FROM {{ source('stg_rmg_kpi', 'raw__rmg_kpi_t_dim_progress_payment') }}
)

SELECT 
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea,
	main_cte.*,
	approval_time = DATEDIFF(DAY, progress_payment_release, reconciliation_release),
	sas_creation_time = DATEDIFF(DAY, rct_approval_control, rec_sas_return),
	billing_time = DATEDIFF(DAY, sd_entry_record, invoice_issuance),
	CASE
		WHEN DATEDIFF(DAY, sd_entry_record, invoice_issuance) IS NULL THEN 'open'
		WHEN DATEDIFF(DAY, sd_entry_record, invoice_issuance) IS NOT NULL THEN 'closed'
	END AS [status]
FROM main_cte 
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} comp ON main_cte.company = comp.RobiKisaKod