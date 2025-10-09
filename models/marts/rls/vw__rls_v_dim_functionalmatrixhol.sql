{{
  config(
    materialized = 'view',tags = ['rls']
    )
}}


SELECT
      [active_directory]
      ,[sf_id_number]
      ,[adines_number] = CAST([adines_number] AS NVARCHAR)
      ,[sap_id] = CAST([sap_id] AS NVARCHAR)
      ,[ad_soyad]
      ,[email]
      ,[rls_profile]
      ,[hr_kpi]
      ,[fi_kpi]
      ,[cash_kpi]
      ,[scm_kpi_bi]
      ,[scm_kpi]
      ,[to_kpi]
      ,[eff_kpi]
      ,[rmore]
      ,[ivf_man]
      ,[ivf_cus]
      ,[bp_kpi]
      ,[co_kpi]
      ,[bd_kpi]
      ,[enrg_kpi]
      ,[ins_kpi]
      ,[nwc_kpi]
      ,[fms_kpi]
      ,[gyg_kpi]
      ,[hos_kpi]
      ,[ed_nwc_kpi]
      ,[hse_kpi]
      ,[metadata_kpi]
      ,[rmh_kpi]
      ,[rgy_kpi]
      ,[ps_kpi]
      ,[rmg_kpi]
      ,[supportops_kpi]
FROM {{ source('stg_rls', 'raw__rls_t_dim_functionalmatrixhol') }}