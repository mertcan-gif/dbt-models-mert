{{
  config(
    materialized = 'table',tags = ['eff_kpi', 'rmore']
    )
}}

/* 
Date: 20250929
Creator: Adem Numan Kaya
Report Owner: Mert Aksoy 
SAP Contact: Mustafa Kilic
Explanation: Cari kayitlarindaki customer bilgilerini istediler, ve bu carilerin ve vendorlarin ne kadar suredir hangi firmalara calistiklari, blocklu olup olmadiklarini gormek istediler. Bank bilgilerini istediler. Rapor bu amacla yayinlanmistir. 
*/


with kunnr_iban as (
  SELECT 
    relation_type = 'Customer'
    ,key_field = 'kunnr'
    ,source_table = 'knbk_iban'
    ,source_key = concat(
                'knbk_iban_',
                CASE WHEN [kunnr] like '0001%' then right([kunnr],7) 
            else [kunnr] end
    ) 
    ,code = 
        CASE WHEN [kunnr] like '0001%' then right([kunnr],7) 
            else [kunnr] end 
    ,data_category = 'IBAN'
    ,tb.[iban] as data_value
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_knbk') }} as knbk
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tiban') }}  as tb on knbk.bankl = tb.bankl and knbk.bankn = tb.bankn and knbk.banks = tb.banks
  )

,kunnr_tax_office as (
  SELECT
    relation_type = 'Vendor'
    ,key_field = 'kunnr'
    ,source_table = 'kna1_taxoffice'
    ,source_key = concat('kna1_taxoffice_',kunnr) 
    ,kunnr
    ,data_category = 'TAX OFFICE'
    ,kna1.stcd1
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} as kna1
)
,kunnr_tax_no as (
  SELECT
    relation_type = 'Vendor'
    ,key_field = 'kunnr'
    ,source_table = 'kna1_taxno'
    ,source_key = concat('kna1_taxno_',kunnr) 
    ,kunnr
    ,data_category = 'TAX NO'
    ,kna1.stcd2
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} as kna1
)

,kunnr_tc_no as (
  SELECT
    relation_type = 'Vendor'
    ,key_field = 'kunnr'
    ,source_table = 'kna1_tcno'
    ,source_key = concat('kna1_tcno_',kunnr) 
    ,kunnr
    ,data_category = 'TC NO'
    ,kna1.stcd3
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} as kna1
)










/* 
KNA_BAK_INFO cte'sinden vendorlarin banka bilgilerine ulasilir. 
*/
,lifnr_iban as (
  SELECT
    relation_type = 'Vendor'
    ,key_field = 'lifnr'
    ,source_table = 'lfbk_iban'
    ,source_key = concat('lfbk_iban_',lifnr) 
    ,code = lifnr
    ,data_category = 'IBAN'
    ,tb.[iban]
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfbk') }} as lfbk
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tiban') }}  as tb on lfbk.bankl = tb.bankl and lfbk.bankn = tb.bankn and lfbk.banks = tb.banks
)

,lifnr_tax_office as (
  SELECT
    relation_type = 'Vendor'
    ,key_field = 'lifnr'
    ,source_table = 'lfa1_taxoffice'
    ,source_key = concat('lfa1_taxoffice_',lifnr) 
    ,lifnr
    ,data_category = 'TAX OFFICE'
    ,lfa1.stcd1
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} as lfa1
)
,lifnr_tax_no as (
  SELECT
    relation_type = 'Vendor'
    ,key_field = 'lifnr'
    ,source_table = 'lfa1_taxno'
    ,source_key = concat('lfa1_taxno_',lifnr) 
    ,lifnr
    ,data_category = 'TAX NO'
    ,lfa1.stcd2
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} as lfa1
)

,lifnr_tc_no as (
  SELECT
    relation_type = 'Vendor'
    ,key_field = 'lifnr'
    ,source_table = 'lfa1_tcno'
    ,source_key = concat('lfa1_tcno_',lifnr) 
    ,lifnr
    ,data_category = 'TC NO'
    ,lfa1.stcd3
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} as lfa1
)

/* 
unionized_table ile cariler ve vendorlarin bilgileri birlestirilir (kna-vendor ve lfa-customer). 
Sonrasi bildigimiz rls. 
*/
,unionized_table_kna1 as (
  select * from kunnr_iban UNION ALL
  select * from kunnr_tax_office UNION ALL
  select * from kunnr_tax_no UNION ALL
  select * from kunnr_tc_no 
)
,unionized_table_lfa1 as (
  select * from lifnr_iban UNION ALL
  select * from lifnr_tax_office UNION ALL
  select * from lifnr_tax_no UNION ALL
  select * from lifnr_tc_no 
)
, unionized_table as (
  select 
    unionized_table_kna1.*
    ,kna1.loevm as deleted_flag
    ,kna1.sperr as blocked_flag
  from unionized_table_kna1
    left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} as kna1 ON unionized_table_kna1.code = kna1.kunnr
  UNION ALL
  select 
    unionized_table_lfa1.*
    ,lfa1.loevm as deleted_flag
    ,lfa1.sperr as blocked_flag
  from unionized_table_lfa1
    left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} as lfa1 ON unionized_table_lfa1.code = lfa1.lifnr
)



select 
  rls_region =  (select dim_rls.rls_region from {{ ref('dm__dimensions_t_dim_companies') }} as dim_rls where dim_rls.RobiKisaKod = 'HOL')
  ,rls_group =  (select dim_rls.rls_group from {{ ref('dm__dimensions_t_dim_companies') }} as dim_rls where dim_rls.RobiKisaKod = 'HOL')
  ,rls_company =  (select dim_rls.rls_company from {{ ref('dm__dimensions_t_dim_companies') }} as dim_rls where dim_rls.RobiKisaKod = 'HOL')
  ,rls_businessarea =  (select dim_rls.rls_businessarea from {{ ref('dm__dimensions_t_dim_companies') }} as dim_rls where dim_rls.RobiKisaKod = 'HOL')
  ,unionized_table.*
from unionized_table
where 1=1
and data_value <> ''