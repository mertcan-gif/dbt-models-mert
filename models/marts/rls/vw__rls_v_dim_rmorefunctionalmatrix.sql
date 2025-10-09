{{
  config(
    materialized = 'view',tags = ['rls']
    )
}}


SELECT
       [active_directory] = 'HOL'
      ,[sf_id_number] = NULL
      ,[adines_number] = NULL
      ,[sap_id] = NULL
      ,rmore.[ad_soyad]
      ,rmore.[email]
      ,dwh.[rls_profile]
      ,rmore.[hr_kpi]
      ,rmore.[fi_kpi]
      ,[cash_kpi] = 'FALSE'
      ,[scm_kpi_bi]  = 'FALSE'
      ,rmore.[scm_kpi]
      ,rmore.[to_kpi]
      ,rmore.[eff_kpi]
      ,rmore.[rmore]
      ,[ivf_man]  = 'FALSE'
      ,[ivf_cus]  = 'FALSE'
      ,rmore.[bp_kpi]  
      ,[co_kpi] = 'FALSE'
      ,[bd_kpi] = 'FALSE'
      ,rmore.[enrg_kpi]
      ,rmore.[ins_kpi]
      ,rmore.[nwc_kpi]
      ,rmore.[fms_kpi]
      ,rmore.[gyg_kpi]
      ,rmore.[hos_kpi]
      ,[ed_nwc_kpi]  = 'FALSE'
      ,rmore.[hse_kpi]
      ,rmore.[metadata_kpi]
      ,rmore.[rgy_kpi]
      ,rmore.[io_kpi]
FROM {{ source('stg_rls', 'raw__rls_t_dim_rmorefunctionalmatrix') }} rmore
  LEFT JOIN {{ source('stg_rls', 'raw__rls_t_dim_functionalmatrixhol') }} dwh on rmore.email = dwh.email