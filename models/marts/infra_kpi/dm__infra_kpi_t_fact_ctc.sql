{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

SELECT 
	rls_region = cm.RegionCode
	,rls_group = CONCAT(cm.KyribaGrup,'_',cm.RegionCode)
	,rls_company = CONCAT(cm.RobiKisaKod,'_',cm.RegionCode)
	,rls_businessarea = CONCAT(TRIM(Proje),'_',cm.RegionCode)
	,TRIM([Proje]) AS project
	,TRIM([Ana Kategori]) AS main_category
	,[date]
	,CAST([Çeyreklik Bütçeye Göre Planlanan] AS FLOAT) planned_amount_by_quaterly_budget
	,CAST([Güncel Bütçe Dönemi Tahakkuk Akışı] AS FLOAT) AS current_budget_period_accrual_flow
	,CAST([Gerçekleşen] AS FLOAT) AS realized_amount
	,CAST([Gerçekleşen Kümülatif] AS FLOAT) AS cumulative_realized_amount
	,CAST([Planlanan İle Gerçekleşen Farkı] AS FLOAT) AS planned_vs_actual_difference
	,CAST([Planlanan Kümülatif Fark] AS FLOAT) AS planned_cumulative_difference
	,CAST([Gerçekleşen Kümülatif Fark] AS FLOAT) AS realized_cumulative_difference
	,CAST([Planlanan İle Gerçekleşen Kümülatif Farkı] AS FLOAT) AS planned_vs_actual_cumulative_difference
	,CAST([Data Control Date] AS DATE) AS data_control_date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_ctc') }} ctc
  LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm on cm.RobiKisaKod = 'REC'

