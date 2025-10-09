{{
  config(
    materialized = 'view',tags = ['eff_kpi']
    )
}}

WITH raw_data as(
SELECT
	[sirket_kodu] AS company_code
	,YEAR(CAST([mali_yil] AS date)) AS fiscal_year
	,[satici_kodu] AS vendor_code
	,[satici_adi] AS vendor_name
	,[musteri_kodu] AS customer_code
	,[musteri_adi] AS customer_name 
	,[cari_kodu] AS current_code
	,CASE 
		WHEN [durum] = '@08@' THEN 'Mutabik'
		WHEN [durum] = '@09@' THEN 'Sonuclanmadi'
		ELSE 'Mutabik Degil'
	END AS reconciliation_statu
	,CAST([ekstre_baslangic_devri] AS float) AS statement_start_period
	,CAST([ekstre_donemi_tutari] AS float) AS statement_period_amount
	,CAST([toplam_tutar] AS float) AS total_amount
	,CASE 
		WHEN [giden_son_mail_tarihi] = '' THEN NULL
		ELSE CONVERT(date, [giden_son_mail_tarihi], 104)
	END AS last_send_email_date
	,CASE 
		WHEN [son_mail_mutabakat_tarihi] = '' THEN NULL
		ELSE CONVERT(date, [son_mail_mutabakat_tarihi], 104)
	END AS last_mail_reconciliation_date  
	,CAST([son_mail_toplam_tutar] AS float) last_mail_total_amount
	,CASE 
		WHEN [gelen_son_cevap_tarihi] = '' 
		THEN NULL
		ELSE CONVERT(date, [gelen_son_cevap_tarihi], 104)
	END AS last_response_date
	,CAST([son_cevap_toplam_tutar] AS float) AS total_amount_last_response
	,CASE 
		WHEN [son_mutabakat_tarihi] = '' 
		THEN NULL
		ELSE CONVERT(date, [son_mutabakat_tarihi], 104)
	END AS last_reconciliation_date
	,CAST([son_mutabakat_toplam_tutar] AS float) AS last_reconciliation_total_amount
	,CONVERT(date, [baslangic_tarihi], 104) AS start_date
	,CONVERT(date, [bitis_tarihi], 104) AS end_date
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfi_030_t_zemut') }}
WHERE 
	/**
	Fevziye Talay Hanımın isteği doğrultusunda kişilerle yapılan mutabakatların çıkarılması için bu filtre konulmuştur.
	**/
	satici_kodu NOT LIKE 'HR%'
	OR LEFT(satici_kodu, 3) LIKE '[A-Za-z][A-Za-z][A-Za-z]'
)
 
SELECT
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,company_code
	,fiscal_year
	,vendor_code
	,vendor_name
	,customer_code
	,customer_name
	,current_code
	,reconciliation_statu
	,statement_start_period
	,statement_period_amount
	,total_amount
	,last_send_email_date
	,last_mail_reconciliation_date
	,last_mail_total_amount
	,last_response_date
	,total_amount_last_response
	,last_reconciliation_date
	,last_reconciliation_total_amount
	,CASE
		WHEN statement_period_amount = 0 AND total_amount = 0 THEN 0
		WHEN statement_period_amount = 0 AND total_amount <> 0 THEN total_amount
		WHEN statement_period_amount <> 0 AND total_amount = 0 THEN statement_period_amount
		ELSE statement_period_amount / total_amount
	 END AS reconciliation_ratio
	,CASE
		WHEN last_reconciliation_total_amount = 0 AND statement_period_amount = 0 THEN 0
		WHEN statement_period_amount = 0 AND last_reconciliation_total_amount <> 0 THEN last_reconciliation_total_amount
		WHEN last_reconciliation_total_amount = 0 AND statement_period_amount <> 0 THEN statement_period_amount
		ELSE last_reconciliation_total_amount / statement_period_amount
	 END AS agreement_reconciliation_ratio
	,start_date
	,end_date
FROM raw_data rw
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON rw.company_code = dim_comp.RobiKisaKod
