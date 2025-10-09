{{
  config(
    materialized = 'table',tags = ['eff_kpi','rmore']
    )
}}

/* 
Date: 20250911
Creator: Elif Erdal & Adem Numan Kaya
Report Owner: Fevziye Talay
SAP Contact: Bekir Elitok
Explanation: Ariba-RES onay akislari disinda SAP uzerinden acilan SAS'larin goruntusu talep edilmektedir. 
*/

with invoice_cte as (

    SELECT  *,ROW_NUMBER() over (partition by ebeln,ebelp order by belnr desc) rn 
    FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rseg') }}  --fatura hareketleri

)
,material_cte as (

    SELECT *,ROW_NUMBER() over (partition by ebeln,ebelp order by mblnr desc) ms --hangi alanları kullanıyorsan onları çağır
    FROM "aws_stage"."s4_odata"."raw__s4hana_t_sap_mseg"  --malzeme hareketleri

)

SELECT 
    [rls_region] = cmp.RegionCode
    ,[rls_group]  = CONCAT(cmp.[KyribaGrup], '_', cmp.RegionCode)
    ,[rls_company]  = CONCAT(ekko.bukrs, '_', cmp.RegionCode)
    ,[rls_businessarea] = CONCAT('_', cmp.RegionCode)
    ,ekko.ebeln as purchase_order_id
    ,ekko.bukrs as company_code
    ,ekko.bstyp as purchase_document_category
    ,ekko.bsart as purchase_document_type
    ,t161t.batxt as purchase_document_type_txt
    ,ekko.ekorg as purchase_order_organization
    ,ekko.ekgrp as purchase_order_group
    ,t024.eknam as purchase_order_group_txt
    ,ekko.FRGGR as approval_group
    ,t16fh.frggt as approval_group_txt
    ,ekko.FRGSX as approval_strategy
    ,t16ft.frgxt as approval_strategy_txt
    ,ekko.ernam as purchase_order_user_info
    ,ekko.zzctr_tolim as tolerance_limit
    ,CAST(ekko.aedat AS DATE)  as purchase_order_created_date
    ,ekpo.ebelp as purchase_item_id
    ,ekpo.knttp as account_assignment_category
    ,t163i.knttx as account_assignment_category_txt
    ,ekpo.txz01 as item_name
    ,CAST(ekpo.netpr AS MONEY) as item_price
    ,CAST(ekpo.netwr AS MONEY) as total_cost
    ,t1.butxt as company_definition
    ,tw.name1 as plant
    ,ekpo.matnr as material_id
    ,CAST(mk.cpudt AS DATE) as material_created_date
    ,mk.usnam as material_user_info
    ,rseg.belnr as invoice_id
    ,CAST(rb.cpudt AS DATE) as invoice_created_date
    ,rb.usnam as invoice_user_info
	,CAST(ekpo.menge as DECIMAL(12,2)) as sas_quantity
	,lfa1.name1 as vendor_name
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekko') }} ekko -- satınalma belge basligi
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekpo') }} ekpo -- satinalma kalemi 
        ON ekpo.ebeln = ekko.ebeln

LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} tw -- üretim yerleri
        ON ekpo.werks = tw.werks

LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001') }} t1 -- sirket kodları
        ON ekko.bukrs = t1.bukrs

LEFT JOIN material_cte mseg -- malzeme hareketleri kalemi
        ON ekpo.ebeln = mseg.ebeln 
            AND ekpo.ebelp=mseg.ebelp
            AND mseg.ms=1  

LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_MKPF') }} mk --malzeme belge baslik
        ON mk.mblnr=mseg.mblnr
            AND mk.mjahr=mseg.mjahr
            
LEFT JOIN invoice_cte rseg

        ON ekpo.ebeln = rseg.ebeln 
            AND ekpo.ebelp = rseg.ebelp 
            AND rseg.rn=1

LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rbkp') }} rb --fatura belgesi baslik
        ON rb.belnr=rseg.belnr
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa1 --tedarikçi ana verileri
	    ON lfa1.lifnr = ekko.lifnr
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t024') }} t024 --satın alma grubu tanımları
	    ON ekko.ekgrp = t024.ekgrp
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t163i') }} t163i --hesap tayini tanımları
	    ON ekpo.knttp = t163i.knttp 
            AND t163i.spras = 'T'
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t161t') }} t161t --sas belge türü tanımı
	    ON ekko.bsart = t161t.bsart 
            AND t161t.spras = 'T'
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t16ft') }} t16ft --onay tanımları
	    ON ekko.frgsx = t16ft.frgsx 
            AND ekko.frggr = t16ft.frggr
            AND t16ft.spras = 'T'
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t16fh') }} t16fh --sas onay grubu tanımları
	    ON ekko.frggr = t16fh.frggr 
            AND t16fh.spras = 'T'
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cmp
        ON ekpo.bukrs=cmp.RobiKisaKod 

WHERE 1=1
    AND ekko.ernam NOT IN (N'RES_PO', N'RFC_ARIBA')
    AND ekpo.loekz = ''               
    AND ekpo.KNTTP in ('A','K','F','M','C','') 
    AND ekko.BSTYP = 'F' 

