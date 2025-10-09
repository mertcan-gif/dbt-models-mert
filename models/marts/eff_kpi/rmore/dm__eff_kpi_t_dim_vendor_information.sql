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


with knbk_cte as (
  SELECT 
    kunnr = 
        CASE WHEN [kunnr] like '0001%' then right([kunnr],7) 
            else [kunnr] end 
    ,knbk.[bankl]
    ,knbk.[bankn]
    ,tb.[iban]
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_knbk') }} as knbk
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tiban') }}  as tb on knbk.bankl = tb.bankl and knbk.bankn = tb.bankn and knbk.banks = tb.banks
  )

,kna_bank_info as (
  SELECT 
    kunnr = knbk_cte.kunnr
    ,bank_information = STRING_AGG('BANKL:' + knbk_cte.bankl + ', BANKN:' + knbk_cte.bankn, ', ' + ' - ')
    ,iban = STRING_AGG('IBAN:' + knbk_cte.iban, ', ' + ' - ')
  FROM knbk_cte
  GROUP BY knbk_cte.kunnr
  )

/* 
KNA_BAK_INFO cte'sinden vendorlarin banka bilgilerine ulasilir. 
*/

,lfa_cte as (
  SELECT
    lifnr
    ,lfbk.[bankl]
    ,lfbk.[bankn]
    ,tb.[iban]
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfbk') }} as lfbk
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tiban') }}  as tb on lfbk.bankl = tb.bankl and lfbk.bankn = tb.bankn and lfbk.banks = tb.banks
)

,lfa_bank_info as (
  SELECT 
    lifnr = lfa_cte.lifnr
    ,bank_information = STRING_AGG('BANKL:' + lfa_cte.bankl + ', BANKN:' + lfa_cte.bankn, ', ' + ' - ')
    ,iban = STRING_AGG('IBAN:' + lfa_cte.iban, ', ' + ' - ')
  FROM lfa_cte
  GROUP BY lfa_cte.lifnr
  )

/* 
LFA_BANK_INFO cte'sinden carilerin banka bilgilerine ulasilir. 
*/

,unionized_table as (
  SELECT 
    lfa1.[lifnr] as vendor_code
    ,lfb1.[bukrs] as company_code
    ,lfa1.[name1] as vendor_customer_name
    ,lfa1.loevm as deleted_flag
    ,lfa1.sperr as blocked_flag
    ,lfb1.sperr as company_blocked_flag
    ,lfa1.stcd1 as tax_number
    ,lfa1.stcd2 as tax_number_2
    ,lfa1.stcd3 as turkist_identity_number
    ,lfa_bank_info.bank_information
    ,lfa_bank_info.iban
    ,[source]='LFA1'
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} AS lfa1 
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfb1') }} as lfb1 on lfb1.lifnr = lfa1.lifnr
  LEFT JOIN lfa_bank_info on lfa1.lifnr = lfa_bank_info.lifnr

union all

  SELECT 
    kna1.[kunnr] as vendor_code
    ,knb1.[bukrs] as company_code
    ,kna1.[name1] as vendor_customer_name
    ,kna1.loevm as deleted_flag
    ,kna1.sperr as blocked_flag
    ,knb1.sperr as company_blocked_flag
    ,kna1.stcd1 as tax_number
    ,kna1.stcd2 as tax_number_2
    ,kna1.stcd3 as turkist_identity_number
    ,kna_bank_info.bank_information
    ,kna_bank_info.iban
    ,[source]='KNA1'
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} AS kna1
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_knb1') }} AS knb1  on kna1.kunnr = knb1.kunnr
  LEFT JOIN kna_bank_info on kna1.kunnr = kna_bank_info.kunnr
)

/* 
unionized_table ile cariler ve vendorlarin bilgileri birlestirilir (kna-vendor ve lfa-customer). 
Sonrasi bildigimiz rls. 
*/

select 
  dim_rls.rls_region as rls_region
  ,dim_rls.rls_group as rls_group
  ,dim_rls.rls_company as rls_company
  ,dim_rls.rls_businessarea as rls_businessarea
  ,rls_key = concat(dim_rls.rls_businessarea, '-', dim_rls.rls_company, '-', dim_rls.rls_group)
  ,dim_rls.KyribaGrup
  ,unionized_table.vendor_code
  ,len(unionized_table.vendor_code) as length_of_code
  ,unionized_table.vendor_customer_name
  ,working_company_code = unionized_table.company_code 
  ,unionized_table.source
  ,unionized_table.deleted_flag
  ,unionized_table.blocked_flag
  ,unionized_table.company_blocked_flag
  ,unionized_table.tax_number
  ,unionized_table.tax_number_2
  ,unionized_table.turkist_identity_number
  ,unionized_table.bank_information
  ,unionized_table.iban
from unionized_table
left join {{ ref('dm__dimensions_t_dim_companies') }} as dim_rls on unionized_table.company_code = dim_rls.RobiKisaKod
where 1=1
  and rls_region is not null