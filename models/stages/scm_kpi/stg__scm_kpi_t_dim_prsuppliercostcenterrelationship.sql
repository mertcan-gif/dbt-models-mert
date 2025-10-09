{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}
WITH main as (
SELECT
    InitialUniqueName  AS pr_name,
    MAX(Supplier_Name) AS Supplier,
    MAX(StatusString) AS status,
    MAX(Name) AS pr_description,
    STRING_AGG([CostCenter.CostCenterDescription], ',') AS cost_center_description,
    STRING_AGG([CostCenter.UniqueName], ',') AS cost_center_name
FROM {{ source('stg_scm_kpi', 'raw__scm_kpi_t_fact_aribarequestswithsuppliers') }}
GROUP BY InitialUniqueName
)
SELECT
  main.pr_name,
  main.Supplier,
  main.pr_description,
  main.status,
  case when main.status = 'Composing' THEN 'Talep Taslağı Oluşturuluyor (Composing)'
       when main.status = 'Submitted' THEN 'Talep Taslağı Oluşturuldu ve Onaya Sunuldu (Submitted)'
       when main.status = 'Denied'  THEN   'Talep İptal Edildi (Denied)' 
       when main.status = 'Ordered'  THEN  'Sipariş Verildi (Ordered)'
       else main.status end as status_explained
FROM main
    