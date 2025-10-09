
{{
  config(
    materialized = 'table',tags = ['duetoduefrom_draft']
    )
}}


SELECT 
  rls_region
  ,rls_group
  ,rls_company
  ,rls_businessarea
  ,business_area_description = BUSINESS_AREA
  ,currency
  ,company = bukrs
  ,business_area = gsber
  ,starting_date_of_month
  ,cumulative_cost = [kümülatif_maliyet]
  ,cumulative_revenue = [kümülatif_gelir]
  ,budget_revene = [butce_geliri]
  ,budget_cost = [butce_maliyet]
  ,budget_currency = [butce_doviz_cinsi]
  ,realization_rate = [gerceklesme_oranı]
  ,expected_revenue = [gerceklesmesi_gereken_gelir]
  ,due
  ,due_type
  ,try_value
  ,eur_value
  ,kyriba_group = KyribaGrup
  ,kyriba_company_code = KyribaKisaKod
FROM {{ ref('stg__nwc_kpi_t_fact_duetoduefrom') }}
