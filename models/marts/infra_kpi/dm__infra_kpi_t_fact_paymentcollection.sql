{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

SELECT 
  rls_region = cm.RegionCode
  ,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
  ,rls_company = cm.KyribaKisaKod + '_' + cm.RegionCode
  ,rls_businessarea = TRIM([Proje Kodu]) + '_' + cm.RegionCode
  ,cm.KyribaGrup as [group]
  ,TRIM([Şirket Kodu]) AS company_code
  ,TRIM([Proje Kodu]) AS project_code
  ,TRIM([Proje Adı]) AS project_shortname
  ,TRIM(CAST([Hakediş No] AS nvarchar)) AS progress_payment_no
  ,[Hakediş Periyodu] AS progress_payment_period
  ,CAST([Hak Doğurucu Tarih (Sözleşme / Fatura)] AS DATE) AS accrual_date
  ,CAST([Gelir (Fatura Tutarı) (BPB)] AS money) AS revenue_bpb
  ,[Fatura Mapping] AS current_revenue_flag
  ,CAST([Gelir (Fatura Tutarı) (EUR)] AS money) AS revenue_eur
  ,CAST([Tahsilat Tarihi] AS date) payment_date
  ,CAST([Tahsilat Raporlama Tarihi] AS date) as reporting_date
  ,TRIM([Belge Para Birimi]) AS document_currency
  ,TRIM([Tahsilatın Konusu]) AS collection_subject
  ,CAST([Tahsilat Tutarı (BPB)] AS money) AS collection_amount_bpb
  ,CAST([Tahsilat Tutarı (EUR)] AS money) AS collection_amount_eur
  ,TRIM([Tahsilat Kaynağı]) AS collection_source
  ,TRIM([Tahsilat Durumu (A)]) AS collection_status
  ,CAST([Güncellik Tarihi] AS date) AS last_modified_date
FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_paymentcollection') }} pc
  LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON TRIM(pc.[Şirket Kodu]) = cm.RobiKisaKod
WHERE [Şirket Kodu] IS NOT NULL