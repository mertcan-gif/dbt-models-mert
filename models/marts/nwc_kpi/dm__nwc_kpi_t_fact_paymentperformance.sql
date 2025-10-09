{{
  config(
    materialized = 'table',tags = ['nwc_kpi_draft','paymentperformance_nwc_draft']
    )
}}

WITH raw_data as(
SELECT 
	kd.[Bukrs] AS company
	,[Lifnr] AS vendor
	,[Name1] AS vendor_name
	,CAST([Budat] AS DATE) AS payment_date
	,[GjahrOd] AS bill_year
	,[Belnr] AS payment_document_no
	,[Umskz] AS general_ledger
	,CAST([Wrbtr] AS float) AS payment_total
	,kd.[Waers] AS currency
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
	,CAST(kd.[db_upload_timestamp] AS datetime) AS db_upload_timestamp
	,DENSE_RANK() OVER (PARTITION BY kd.Bukrs, Lifnr, BelnrFatura, GjahrFa, Belnr,Budat ORDER BY kd.db_upload_timestamp desc) as rn
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kalemdetayset') }} kd
		LEFT JOIN {{ ref('vw__s4hana_v_sap_ug_t001') }} t001 ON kd.bukrs = t001.bukrs
  WHERE 1=1 
  	AND [Zfbdt] IS NOT NULL
	AND t001.waers = 'TRY'
	AND CAST([Budat] AS DATE) >= '2024-01-04'
	AND LEN([Lifnr]) <> 3
	AND LEFT([Lifnr],1) <> '5' 
	AND LEFT([Lifnr],1) <> '6'
)

SELECT 
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(r.company ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(r.work_area,''),'_',COALESCE(kuc.RegionCode,''))
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
	,interest_rate_try = f.rate1
	,amount_of_interest
	,weight_value
	,r.[db_upload_timestamp]
	,kuc.KyribaGrup
	,kuc.KyribaKisaKod
FROM raw_data r
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON r.company = kuc.RobiKisaKod
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_yfin_vdl_t_012') }} f
		ON f.datum = (
			SELECT MAX(datum) 
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_yfin_vdl_t_012') }} f 
			WHERE f.datum <= r.payment_date
		)
		AND f.waers = 'TRY'
WHERE rn = 1