{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}

/* 
Date: 20250915
Creator: Adem Numan Kaya
Report Owner: Hilal Sena Gulec
Explanation: 2025 yilinda hakedisleri tamamlanmis olan ve 100k usd uzerinde hakedisi olan tablolari vendorlar istenmistir. Bu listelenmistir.
*/

with finals as (
    SELECT 
    [WERKS] as business_area
    ,haktut.[LIFNR] as vendor_code
    ,hakedis.[LIFNR_TXT] as vendor
    ,haktut.[EBELN] as contract_number
    ,haktut.[HAKEDISNO] as progress_payment_no
    ,haktut.[WAERS] as currency 
    ,haktut.[ONAYCITNM] as approver
    ,cast(haktut.[GOITAB102] as money) as progress_payment_amount
    ,hakedis.[DURUM] as [status]
    ,CAST(hakedis.[BASTARIH] AS DATE) as [start_date]
    ,CAST(hakedis.[BITTARIH] AS DATE) AS [end_date]
    ,normalized_progress_payment_amount_usd=
        case 
            when waers = 'TRY' then CAST(cast([GOITAB102] as money)/dailycurr.try_value as money) 
            when waers = 'EUR' then CAST(cast([GOITAB102] as money)/dailycurr.eur_value as money)
            when waers = 'USD' then CAST(cast([GOITAB102] as money)/1 as money)
        end
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_haktut') }} as haktut
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hakedis') }} as hakedis 
    on hakedis.ebeln=haktut.ebeln 
    and haktut.HAKEDISNO=hakedis.HAKEDISNO 
    and hakedis.KESINTEMINAT='1'
    left join {{ ref('stg__dimensions_t_dim_dailys4currencies') }} dailycurr 
    on dailycurr.date_string=hakedis.BITTARIH and dailycurr.currency='USD'
)

select 
dim_projects.rls_region
,dim_projects.rls_group
,dim_projects.rls_company
,dim_projects.rls_businessarea
,rls_key=concat(rls_businessarea,'-',rls_company,'-',rls_group)
,finals.*
from finals
left join {{ ref('dm__dimensions_t_dim_projects') }} as dim_projects on dim_projects.business_area=finals.business_area
where 1=1 
    and rls_businessarea is not null
    and vendor is not null 
    AND normalized_progress_payment_amount_usd > 100000
    AND YEAR(end_date) = 2025