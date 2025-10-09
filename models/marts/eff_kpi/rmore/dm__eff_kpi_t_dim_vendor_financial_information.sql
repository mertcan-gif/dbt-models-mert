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
Explanation: Carideki firmalarin finansal bilgilerini, dip toplamlarini, son kayit tarihlerini, bakiye toplamlarini gormek istediler. Rapor bu amacla yayinlanmistir. 
*/

with account_descriptions as ( 
    SELECT saknr,txt50
    FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_skat') }}
        where ktopl = 'RONS' AND spras = 'T'
  ) 

,_finals_raw as (
select
    acdoca.rbukrs
	,account_descriptions.txt50 as racct_description
	,LEFT(acdoca.racct,1) as racct_first
	,LEFT(acdoca.racct,2) as racct_first_two
	,LEFT(acdoca.racct,3) as racct_first_three
	,CAST(acdoca.wsl AS MONEY) as wsl
	,CAST(acdoca.hsl AS MONEY) as hsl
	,CAST(acdoca.ksl AS MONEY) as ksl
	,cast(acdoca.budat as date) as budat
	,cast(acdoca.bldat as date) as bldat
	,acdoca.lifnr
    ,acdoca.belnr
    ,lfa1.name1 as lifnr_name
	,acdoca.kunnr
    ,kna1.name1 as kunnr_name
from {{ ref('stg__s4hana_t_sap_acdoca_full') }} acdoca
	LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf ON acdoca.belnr = bkpf.belnr
										AND acdoca.rbukrs = bkpf.bukrs
										AND acdoca.gjahr = bkpf.gjahr
	LEFT JOIN account_descriptions on acdoca.racct = account_descriptions.saknr
    left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa1 on acdoca.lifnr = lfa1.lifnr
    left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} kna1 on acdoca.kunnr = kna1.kunnr
where 1=1
	and bkpf.xreversed = 0
	and bkpf.xreversing = 0
	and (acdoca.lifnr <> '' or acdoca.kunnr <> '')
	and cast(acdoca.budat as date) <= '2025-06-30'
)

select
    comp.rls_region
	,comp.[rls_group]  
	,comp.rls_company
	,rls_businessarea = CONCAT('_', comp.rls_region)
    ,rls_key = CONCAT('_', comp.rls_region, '-', comp.rls_company, '-', comp.[rls_group])
    ,company_code = rbukrs 
    ,customer_code = lifnr
	,len(lifnr) as length_of_customer_code
    ,vendor_code = kunnr
    ,customer_name = lifnr_name
	,len(kunnr) as length_of_vendor_code
    ,vendor_name = kunnr_name
    ,account_first = racct_first
    ,account_first_two = racct_first_two
    ,account_first_three = racct_first_three
    ,total_amount_document_currency = sum(wsl)
    ,total_amount_company_currency = sum(hsl)
    ,total_amount_global_currency = sum(ksl)    
    ,latest_document_date = max(budat)
    ,total_documents_count = count(distinct(belnr))
from _finals_raw
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} comp ON rbukrs = comp.RobiKisaKod
WHERE 1=1
    and comp.rls_region is not null
GROUP BY comp.rls_region
		,comp.[rls_group]  
		,comp.rls_company
		,CONCAT('_', comp.rls_region)
		,CONCAT('_', comp.rls_region, '-', comp.rls_company, '-', comp.[rls_group])
		,rbukrs 
		,lifnr
		,kunnr
		,lifnr_name
		,kunnr_name
		,racct_first
		,racct_first_two
		,racct_first_three