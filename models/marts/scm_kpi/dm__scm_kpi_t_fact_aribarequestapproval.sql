
{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}

SELECT
       [rls_key]
      ,[rls_region]
      ,[rls_group]
      ,[rls_company]
      ,[rls_businessarea]
      ,[talep_numarasi] as code
      ,[sirket] as company
      ,[proje] as project
      ,talep_konusu as request_subject
      ,project_name
      ,status
      ,country
      ,satınalma_grubu_uzun as procurement_group_long_name
      ,[satinalma_personeli] as procurement_personnel
      ,[satinalma_grubu] as procurement_group
      ,[talep_olusturma_tarihi] as request_creation_date
      ,[talep_onaya_sunma_tarihi] as request_submission_date
      ,[talep_onay_tarihi] as request_approval_date
      ,[islem_suresi_onayci_gun] as process_timelapse
      ,[talep_onay_tamamlanma_tarihi] as request_completion_date
      ,[talep_onay_tamamlanma_suresi_gun] as request_completion_timelapse
      ,[surec] as process
      ,onaycı_sayısı as number_of_approver
      ,satınalmacı_gercek as real_procurement_personel
      ,document_type 
FROM {{ ref('stg__scm_kpi_t_fact_aribarequestapproval') }} m
where rls_businessarea is not null