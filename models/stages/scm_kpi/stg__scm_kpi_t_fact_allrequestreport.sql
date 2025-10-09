{{
  config(
    materialized = 'table',
    tags = ['scm_kpi']
  )
}}
WITH tdf_satinalma_ilk_onay AS (
    SELECT
        tdf_id,
        MIN( [TDF Satınalma Onay Verme Tarihi]) as tdf_satinalma_ilk_onay
    FROM "aws_stage"."scm_kpi"."stg__scm_kpi_t_fact_aribapefapproval"
    WHERE 1=1
        AND tdf_process_id = '1'
    GROUP BY tdf_id
),
 
tdf_satinalma_ilk_onay_tamamlanma AS (
    SELECT
        tdf_id,
        MAX([TDF Satınalma Onay Verme Tarihi]) as tdf_satinalma_onay_tamamlanma_tarih
    FROM "aws_stage"."scm_kpi"."stg__scm_kpi_t_fact_aribapefapproval"
    WHERE 1=1
        AND tdf_process_id = '1'
    GROUP BY tdf_id
),
 
tdf_yapim_ilk_onay AS (
    SELECT
        tdf_id,
        MIN([TDF Yapım Onay Verme Tarihi]) as tdf_yapim_ilk_onay
    FROM "aws_stage"."scm_kpi"."stg__scm_kpi_t_fact_aribapefapproval"
    WHERE 1=1
        AND tdf_process_id = '2'
    GROUP BY tdf_id
),
 
tdf_yapim_son_onay AS (
    SELECT
        tdf_id,
        MAX([TDF Yapım Onay Verme Tarihi]) as tdf_yapim_son_onay
    FROM "aws_stage"."scm_kpi"."stg__scm_kpi_t_fact_aribapefapproval"
    WHERE 1=1
        AND tdf_process_id = '2'
    GROUP BY tdf_id
),
 
kontrat_yaratilma AS (
    SELECT
        id,
        process_date
    FROM "aws_stage"."scm_kpi"."stg__scm_kpi_t_fact_processingtimes"
    WHERE 1=1
        AND tdf_approval_group = 'Kontrat Yaratılma Tarihi'
),
 
siparis_cevirme AS (
    SELECT
        MAX(badat) as badat,
        zzaribareqno
    FROM "aws_stage"."s4_odata"."raw__s4hana_t_sap_eban"
    WHERE 1=1
        AND badat != ''
        AND zzaribareqno != ''
    GROUP BY zzaribareqno
),

siparis_toplamı as (
SELECT 
    SUM(CAST(t.netpr AS FLOAT)) AS kur_bazlı_sas_toplamı,
    e.waers AS kur,
    STRING_AGG(CAST(t.ebeln AS NVARCHAR(MAX)), ',') AS ebeln_list,
    STRING_AGG(CAST(t.knttp AS NVARCHAR(MAX)), ',') AS knttp_list,
	eb.zzaribareqno ,
    e.lifnr
FROM aws_stage.s4_odata.raw__s4hana_t_sap_ekpo t
LEFT JOIN aws_stage.s4_odata.raw__s4hana_t_sap_ekko e 
LEFT JOIN  ( SELECT DISTINCT
					ebeln,
					zzaribareqno,
					knttp
			FROM aws_stage.s4_odata.raw__s4hana_t_sap_eban
			WHERE 1=1 
				and ebeln != ''
				and ebeln is not null
				and zzaribareqno != ''
				and zzaribareqno is not null
				--and zzariba_req_no = 'PR13847' 
				) eb on eb.ebeln = e.ebeln
    ON e.ebeln = t.ebeln
WHERE 
    1 = 1
    AND LTRIM(RTRIM(t.ebeln)) != ''
    AND t.ebeln IS NOT NULL
    AND e.lifnr IS NOT NULL
    AND LTRIM(RTRIM(e.lifnr)) != ''
	--and eb.zzariba_req_no = 'PR13847'
    --AND t.ebeln = '7000010714'
GROUP BY 
    t.ebeln,
    e.waers,
    e.lifnr,
	eb.zzaribareqno

)
,sup_total_price AS (
    SELECT
        tdf_id,
        invitation_id as supplier,
		tc.org_name as vendor_name,
		tc.org_erpVendorID as vendor_id,
		event_id, 
        unit_price_currency as totalpricecurrency,
        SUM(CAST(awarded_price_total AS FLOAT)) as totalprice
    FROM  "aws_stage"."s4_odata"."raw__s4hana_t_sap_ariba_zmm_t_tdf_snd_lg"
	left join  aws_stage.scm_kpi.raw__scm_kpi_t_fact_ariba_tendercompanies tc on tc.invitationId = invitation_id and tc.doc_id= event_id
	where 1=1
	--and tdf_id = '1000005755'
    GROUP BY tdf_id, invitation_id, unit_price_currency,event_id,tc.org_name,tc.org_erpVendorID
),
butce AS (
    SELECT
        tdf_id,
        SUM(CAST([butce] AS FLOAT)) as butce,
        supplier
 FROM "aws_stage"."s4_odata"."raw__s4hana_t_sap_zmm_t_tdf_itm_cs" t
    GROUP BY tdf_id, supplier
),
 
vendor_names AS (
    SELECT
        systemid,
        vendorid,
        l.name1 as vendor_name
    FROM "aws_stage"."s4_odata"."raw__s4hana_t_sap_ariba_arbcig_systidmap" k  
        LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_lfa1" l ON RIGHT(k.vendorid, 7) = l.lifnr
),
 
catlog AS (
    SELECT
        tdf_id,
        invitation_id,
        answer
    FROM "aws_stage"."s4_odata"."raw__s4hana_t_sap_ariba_zmm_t_tdf_catlog"
    WHERE question_id = 'PAYMENT_TERMS'
),
 
rfc AS (
    SELECT
        k.ariba_req_no,
        STRING_AGG(k.uname, ',') AS unames
    FROM (
        SELECT DISTINCT
            ariba_req_no,
            uname,
			db_upload_timestamp
        FROM "aws_stage"."s4_odata"."raw__s4hana_t_sap_ariba_zmm_pr_arb_log"
    ) k
    GROUP BY ariba_req_no
),
 
sat_no AS (
    SELECT
        zzaribareqno,
        STRING_AGG(banfn, ',') AS banfn
    FROM (
        SELECT DISTINCT zzaribareqno, banfn
        FROM "aws_stage"."s4_odata"."raw__s4hana_t_sap_eban"
        WHERE zzaribareqno != ''
    ) t
    GROUP BY zzaribareqno
),
 
satinalma_personel AS (
    SELECT
        zzariba_req_no,
        MAX(CONCAT(erdat,'_',erzet)) as satinalma_personel_atanma_tarihi
    FROM "aws_stage"."s4_odata"."raw__s4hana_t_sap_ariba_zarb_t_701"
    WHERE STATUS_REP = '300'
    GROUP BY zzariba_req_no
),
masraf_ve_ic_siparis as (

SELECT 
ekkn.ebeln as sas,
STRING_AGG(ekkn.kostl,',') as cost_center,
STRING_AGG(ekkn.aufnr,',') as internal_order
  FROM ( select distinct 
					 ebeln
					,kostl 
					,aufnr
	FROM [aws_stage].[s4_odata].[raw__s4hana_t_sap_ekkn]) ekkn
group by ekkn.ebeln
),
 
siparis_detay AS (
    SELECT
        STRING_AGG(de.ebeln, ',') AS sas_numarasi,
		STRING_AGG(sd.cost_center,',') AS masraf_yeri,
		STRING_AGG(sd.internal_order,',') AS ic_siparis,
        de.zzaribareqno AS birinci_pr,
        MAX(DATEADD(SECOND, 86399, CAST(CAST(e.aedat AS DATE) AS DATETIME))) AS process_date,
        de.banfn AS sat_numarasi,
        de.lifnr AS vendor_id,
		vendor.systemid
    FROM (
        SELECT
            DISTINCT eb.zzaribareqno,
            eb.banfn,
            e.lifnr,
            eb.ebeln
        FROM "aws_stage"."s4_odata"."raw__s4hana_t_sap_eban" eb
        LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_ekko" e
            ON e.ebeln = eb.ebeln
        WHERE e.aedat IS NOT NULL AND e.aedat != ''
    ) de
    LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_ekko" e
        ON e.ebeln = de.ebeln
	LEFT JOIN 
		(    SELECT
        systemid,
        lifnr,
        l.name1 as vendor_name
    FROM "aws_stage"."s4_odata"."raw__s4hana_t_sap_ariba_arbcig_systidmap" k  
        LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_lfa1" l ON RIGHT(k.vendorid, 7) = l.lifnr) vendor on vendor.lifnr = e.lifnr
		LEFT JOIN masraf_ve_ic_siparis sd on sd.sas = de.ebeln
    where de.zzaribareqno != ''
    GROUP BY
        de.zzaribareqno,
        de.banfn,
        de.lifnr,
		vendor.systemid
),
 
sozlesme_yaratma AS (
    SELECT
        contract_id,
        contract_name,
        tdf_id,
        supplier_id,
        supplier_name,
        cowid,
        TRY_CAST(CONCAT(crdat,' ',crtim) AS DATETIME) contract_creation_date
 FROM "aws_stage"."s4_odata"."raw__s4hana_t_sap_ariba_zarb_t_100"
    WHERE 1=1 --cowid <> ''
	and cowid <> ''
),
doc_types as (
SELECT DISTINCT
zzaribareqno,
CASE	
	WHEN knttp = ''  THEN 'Stoklu Alım'
	WHEN knttp = 'B' THEN 'Dp.ürt/mşt.sprş çğr'
	WHEN knttp = 'C' THEN 'Dp.ürt/mşt.sprş çğr'
	WHEN knttp = 'D' THEN  'Spr.üz.ürt/pr.msf.yk'
	WHEN knttp = 'E' THEN  'KD-CO ile sprş.üz.'
	WHEN knttp = 'F' THEN  'Sipariş'
	WHEN knttp = 'G' THEN  'Depoya üretim/proje'
	WHEN knttp = 'K' THEN  'Masraf yeri'
	WHEN knttp = 'M' THEN  'KD-CO olm.sprş.üz.'
	WHEN knttp = 'N' THEN  'Ağ planı'
	WHEN knttp = 'P' THEN  'Proje'
	WHEN knttp = 'Q' THEN  'Sprş.üzrn.ürt(proje)'
	WHEN knttp = 'T' THEN  'Tüm yeni yan msf.'
	WHEN knttp = 'U' THEN  'Tanınmıyor'
	WHEN knttp = 'X' THEN  'Tüm yardımcı hsp.tyn'
	WHEN knttp = 'Y' THEN  'Proje Aktivite Tşrn'
	WHEN knttp = 'Z' THEN  'İade edlb.ambalaj'
END AS document_type
from "aws_stage"."s4_odata"."raw__s4hana_t_sap_eban" 
where zzaribareqno != ''
)
 
SELECT distinct
   
    CONCAT(prj.rls_businessarea, '-', prj.rls_company, '-', prj.rls_group) as rls_key,
    prj.rls_region,
    prj.rls_group,
    prj.rls_company,
    prj.rls_businessarea,
    prj.company,
    main_table.werks AS business_area,
    main_table.bnfpo as line_item,
    country.land1 as country,
    main_table.ZZBTP_USER as request_creator,
    a.Tanim as company_name,
    t1.name1 as project,
    main_table.zzariba_header_txt as request,
    CASE
        when catalogs.status  = 'Denied' THEN catalogs.status_explained
        WHEN r.status = 'C' THEN 'TDF Olusturuldu'
        WHEN r.status = 'S' THEN 'TDF Onaya Sunuldu'
        WHEN r.status = 'R' THEN 'TDF Reddedildi-PO'
        WHEN r.status = 'B' THEN 'TDF Geri Gönderildi -DM'
        WHEN r.status = 'A' THEN 'TDF Onaylandı'
        WHEN r.status = 'D' THEN 'TDF Iptal Edildi'
        --when catalogs.status  = 'Denied' THEN catalogs.status_explained
		WHEN r.status is null then catalogs.status_explained

    END as [status],
    main_table.zzariba_req_no as first_pr_number,
    r.created_pr_id as second_pr_number,
    r.tdf_id,
    r.satinalma_yetkilisi as procurement_personnel,
    main_table.eknam as procurement_group,
    main_table.process_date as request_creation_date,
    main_table.process_date as request_submission_date,
    main_table.max_process_date as request_completion_date,
    satinalma_personel_atanma.satinalma_personel_atanma_tarihi as procurement_personnel_assignment_date,
    main_table.badat as procurement_group_assignment_date,
    CAST(LEFT(at1.[createDate], 23) AS datetime) AS tender_creation_date,
    CAST(LEFT(at1.[openDate], 23) AS datetime) as tender_start_date
    ,CAST(LEFT(at1.closeDate, 23) AS datetime)  as tender_completion_date
    ,CAST(
        DATEDIFF_BIG(
            SECOND,
            CAST(LEFT(at1.[createDate], 23) AS datetime),
            TRY_CAST(r.created_date + ' ' + r.created_time AS datetime)
        ) AS FLOAT) / 86400 AS tender_elapsed_time,
    at1.commodity_name as tender_activity_area,
    TRY_CAST(r.created_date + ' ' + r.created_time AS datetime) AS pef_creation_date,
    tk.tdf_onaya_sunma_tarih as pef_submission_date,
    j.tdf_satinalma_ilk_onay as pef_purchase_approval_date,
    j1.tdf_satinalma_onay_tamamlanma_tarih as pef_purchase_approval_completion_date,
    CAST(
        DATEDIFF_BIG(
            SECOND,
            tk.tdf_onaya_sunma_tarih,
            j1.tdf_satinalma_onay_tamamlanma_tarih
        ) AS FLOAT) / 86400 pef_purchase_elapsed_time,
    k2.tdf_yapim_ilk_onay as pef_construction_approval_date,
    k3.tdf_yapim_son_onay as pef_construction_approval_completion_date,
    CAST(
        DATEDIFF_BIG(
            SECOND,
            k2.tdf_yapim_ilk_onay,
            k3.tdf_yapim_son_onay
        ) AS FLOAT) / 86400 pef_construction_elapsed_time,
    sozlesme_yaratma.contract_creation_date as contract_creation_date,
    sozlesme_yaratma.cowid as contract_id,
    siparis_cevirme_tarih.badat as order_conversion_date, --COALESCE(siparis_cevirme_tarih.badat, siparis_detay.process_date) as order_conversion_date,
    pr2_siparis_onay.record_Date as order_submission_date,
    pr2_siparis_onay.record_Date as order_completion_date,
    TRY_CONVERT(DECIMAL(18, 5),
                REPLACE(REPLACE(r.secilen_teklifler_toplami, '.', ''), ',', '.')) as seleted_offer_total,
    r.para_birimi as selected_offer_total_currency,
    r.usd_cevirim as usd_currency,
    TRY_CONVERT(DECIMAL(18, 5),
            REPLACE(REPLACE(r.bidding_amount, '.', ''), ',', '.')) as pef_total_price_usd,
    NULL as total_price_try,
    sup_total_price.totalprice AS selected_company_total,
    sup_total_price.totalpricecurrency AS selected_company_total_currency,
    main_table.zzariba_header_txt as order_name,
    st.kur_bazlı_sas_toplamı AS order_total,
    st.kur AS order_total_currency,
    NULL AS order_total_usd,
    kur_tablosu.usd_try_value AS usd_currency_value_s4,
    kur_tablosu.eur_try_value AS eur_currency_value_s4,
    NULL AS e_purchase_type,
    butce.butce AS budget,
    --coalesce(vendor_names.vendor_name,vendor_names2.vendor_name) as selected_company,
    catlog.answer as payment_terms,
	sup_total_price.vendor_name as selected_company,
    dt.document_type AS document_type,
    siparis_detay2.masraf_yeri AS cost_center,
    siparis_detay2.ic_siparis AS internal_order,
    st.knttp_list AS account_settlement,
    r.purchase_type AS purchase_type,
    CASE
        WHEN rfc_ariba_mi.ariba_req_no IS NOT NULL THEN 'BTP-Excel Upload'
		when catalogs.Supplier is null then  'BTP-TDFden yaratılmıs'
        --WHEN rfc_ariba_mi.ariba_req_no IS NULL AND created_pr_id = '' AND sup_total_price.supplier IS NOT NULL AND sup_total_price.supplier != '' THEN 'BTP-Katalogdan yaratılmıs'
    END as source_module,
    sat_no.banfn as sat_id,
    cerceve_sozlesme.ebeln as frame_contract_id,
    siparis_detay2.sas_numarasi as sas_id,
    sup_total_price.vendor_id as vendor_id,
    ihale_olusturma.docid as tender_id,
    sr_yaratma.srid as sr_id
FROM "aws_stage"."scm_kpi"."stg__scm_kpi_t_mapping_main_table_lowered" main_table  
    LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_ariba_zarb_t_509t" sr_yaratma ON sr_yaratma.requisitionnumber = main_table.zzariba_req_no
    LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_ariba_zarb_t_511T" ihale_olusturma ON sr_yaratma.srid = ihale_olusturma.srid
    LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_zmm_t_tdf_header" r 
        ON r.talep_sap_id = main_table.zzariba_req_no AND r.status != 'D' and r.doc_id = ihale_olusturma.docid
    LEFT JOIN "aws_stage"."scm_kpi"."stg__scm_kpi_t_mapping_tdf_onaya_sunma" tk ON tk.tdf_id = r.tdf_id
    LEFT JOIN "dwh_prod"."dimensions"."dm__dimensions_t_dim_projects" prj ON prj.[business_area] = main_table.werks
    LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_t001w" as country ON country.werks = prj.business_area
    LEFT JOIN tdf_satinalma_ilk_onay j ON j.tdf_id = r.tdf_id
    LEFT JOIN tdf_satinalma_ilk_onay_tamamlanma j1 ON j1.tdf_id = r.tdf_id
    LEFT JOIN tdf_yapim_ilk_onay k2 ON k2.tdf_id = r.tdf_id
    LEFT JOIN tdf_yapim_son_onay k3 ON k3.tdf_id = r.tdf_id
    LEFT JOIN kontrat_yaratilma m ON m.id = r.tdf_id
    LEFT JOIN siparis_cevirme siparis_cevirme_tarih ON siparis_cevirme_tarih.zzaribareqno = r.created_pr_id
    LEFT JOIN sup_total_price sup_total_price ON sup_total_price.tdf_id = r.tdf_id and sup_total_price.event_id= r.doc_id
    LEFT JOIN butce butce ON butce.tdf_id = r.tdf_id AND butce.supplier = sup_total_price.supplier
  --LEFT JOIN vendor_names vendor_names ON vendor_names.systemid = sup_total_price.supplier
    LEFT JOIN catlog catlog ON catlog.tdf_id = r.tdf_id AND sup_total_price.supplier = catlog.invitation_id
    LEFT JOIN rfc rfc_ariba_mi ON rfc_ariba_mi.ariba_req_no = main_table.zzariba_req_no

    LEFT JOIN sat_no sat_no ON sat_no.zzaribareqno = main_table.zzariba_req_no
    LEFT JOIN satinalma_personel satinalma_personel_atanma ON satinalma_personel_atanma.zzariba_req_no = main_table.zzariba_req_no    
    LEFT JOIN "aws_stage"."scm_kpi"."raw__scm_kpi_t_fact_aribaprocurementrequestsjobresult2" pr2_siparis_onay ON pr2_siparis_onay.meta_InitialUniqueName = r.created_pr_id
    --LEFT JOIN siparis_detay siparis_detay ON siparis_detay.birinci_pr = main_table.zzariba_req_no and siparis_detay.systemid = sup_total_price.supplier
    LEFT JOIN siparis_detay siparis_detay2 ON siparis_detay2.birinci_pr = r.created_pr_id and siparis_detay2.systemid = sup_total_price.supplier
    --LEFT JOIN vendor_names as vendor_names2 ON  right(vendor_names2.vendorid,7) = siparis_detay.vendor_id
    LEFT JOIN sozlesme_yaratma as sozlesme_yaratma
        ON sozlesme_yaratma.tdf_id = r.tdf_id
        AND sup_total_price.supplier = sozlesme_yaratma.supplier_id
    LEFT JOIN "aws_stage"."scm_kpi"."stg__scm_kpi_t_mapping_kur_tablosu" kur_tablosu ON tk.process_date = kur_tablosu.date_value
    LEFT JOIN "aws_stage"."scm_kpi"."stg__scm_kpi_t_mapping_cerceve_sozlesme"  cerceve_sozlesme ON cerceve_sozlesme.cowid = sozlesme_yaratma.contract_id  
    LEFT JOIN "dwh_prod"."dimensions"."dm__dimensions_t_dim_companies" a on a.RobiKisaKod =  prj.company
    LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_t001w" t1 on t1.werks = main_table.werks
    LEFT JOIN (select zzaribareqno,
                      string_agg(document_type,',') document_type
                from  doc_types group by zzaribareqno) dt on dt.zzaribareqno = main_table.zzariba_req_no
	LEFT JOIN [aws_stage].[scm_kpi].[raw__scm_kpi_t_dim_aribatenders] at1 on at1.internalId = ihale_olusturma.docid
	LEFT JOIN siparis_toplamı st on st.zzaribareqno = r.created_pr_id and st.kur = sup_total_price.totalpricecurrency and st.lifnr = RIGHT(sup_total_price.vendor_id,7)
	right JOIN aws_stage.scm_kpi.stg__scm_kpi_t_dim_prsuppliercostcenterrelationship catalogs on catalogs.pr_name = main_table.zzariba_req_no and catalogs.Supplier is null
WHERE 1=1
    AND main_table.zzariba_req_no IS NOT NULL
    --AND main_table.zzariba_req_no = 'PR24795'
	--and contract_id= 'CW21384'
and  main_table.zzariba_req_no NOT IN  
						(
						select distinct
						created_pr_id
						 from aws_stage.s4_odata.raw__s4hana_t_sap_zmm_t_tdf_header ) --ikinci pr ı dışarıda bıraktım
and catalogs.Supplier is null -- kataloglu alımları dışarıda bıraktım