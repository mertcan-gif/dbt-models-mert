{{
  config(
    materialized = 'view',tags = ['eff_kpi']
    )
}}

WITH raw_data as(
SELECT 
	[Bukrs] AS company
	,[Lifnr] AS vendor
	,[Name1] AS vendor_name
	,CAST([Budat] AS DATE) AS payment_date
	,[GjahrOd] AS bill_year
	,[Belnr] AS payment_document_no
	,[Umskz] AS general_ledger
	,CAST([Wrbtr] AS float) AS payment_total
	,[Waers] AS currency
	,CAST([Dmbtr] AS float) AS payment_total_try
	,[Gsber] AS work_area
	,[GsberText] AS work_area_name
	,[GjahrFa] AS fiscal_year
	,[BelnrFatura] AS invoice_no_in_sap_fi
	,CAST([Zfbdt] AS date) AS invoice_start_date
	,[Zbd1t] AS remaining_term_day
	,CAST([FaturaVade] AS date) AS remaining_term_date 
	,[HBlart] AS document_type_of_invoice 
	,[BlartText] AS document_type_name_of_invoice
	,[Ebeln] AS sas_no
	,[BelgeVade] AS remaining_term_day_for_sas_no
	,[VadeGun] AS maturity
	,CAST([Tsatz] AS float) AS interest_rate
	,CAST([FaizTutar] AS float) AS amount_of_interest
	,CAST([AgirlikTutar] AS float) AS weight_value
	,CAST([db_upload_timestamp] AS datetime) AS db_upload_timestamp
	,DENSE_RANK() OVER (PARTITION BY Bukrs, Lifnr, BelnrFatura, GjahrFa, Belnr,Budat ORDER BY db_upload_timestamp desc) as rn
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kalemdetayset') }}
)

SELECT
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea = CONCAT(work_area, '_' , rls_region) 
	,company
	,vendor
	,vendor_name
	,payment_date
	,bill_year 
	,payment_document_no
	,general_ledger
	,payment_total
	,currency
	,payment_total_try
	,work_area
	,work_area_name
	,fiscal_year
	,invoice_no_in_sap_fi
	,invoice_start_date
	,remaining_term_day
	,remaining_term_date
	,document_type_of_invoice
	,document_type_name_of_invoice
	,sas_no
	,remaining_term_day_for_sas_no
	,maturity
	,interest_rate
	,amount_of_interest
	,weight_value
	,[db_upload_timestamp]
FROM raw_data rw
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON rw.company = dim_comp.RobiKisaKod
WHERE rn = 1