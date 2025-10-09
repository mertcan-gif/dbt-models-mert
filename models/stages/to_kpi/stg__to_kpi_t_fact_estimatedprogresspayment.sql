{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
WITH raw_data AS (
    SELECT
		company = proj.vbukr
        ,project_code = proj.pspid
        ,project_name = proj.post1
        ,contract = zzctr_aszno
        ,document_no = ekko.ebeln
		,subcontractor_no = ekko.lifnr
        ,subcontractor_name = lfa1.name1
        ,currency = ekko.waers
        ,progress_payment_no = CAST(hakedis.HAKEDISNO AS int)
        ,progress_payment_statu = hakedis.DURUM
        ,progress_payment_start_date = CAST(hakedis.bastarih AS date)
        ,progress_payment_end_date = CAST(hakedis.bittarih AS date)
        ,completed_progress_payment_flag =
                            CASE WHEN hakedis.DURUM = '04' AND hakedis.KESINTEMINAT = '1' THEN 1
                            ELSE 0 END
        ,exact_progress_payment_flag =
                                    CASE WHEN hakedis.KESINTEMINAT = '1' THEN 1
                                    ELSE 0 END
        ,company_preparation_flag =
                            CASE WHEN hakbil.GECKABULYAPILDIMI = '1' AND hakbil.KESINHAKDURUMU = '2' THEN 1
                            ELSE 0 END
        ,worksite_control_flag =  
                        CASE WHEN hakbil.GECKABULYAPILDIMI = '1' AND hakbil.KESINHAKDURUMU = '1' THEN 1
                        ELSE 0 END
        ,estimated_progress_payment_try = CAST(COALESCE(hakbil.TAHKALANHAKTRY, '0') as money)
        ,estimated_progress_payment_usd = CAST(COALESCE(hakbil.TAHKALANHAKUSD, '0') as money)
        ,estimated_progress_payment_eur = CAST(COALESCE(hakbil.TAHKALANHAKEUR, '0') as money)
        ,company_prepared_estimated_progress_payment_flag =  
                                                CASE WHEN hakbil.KESINHAKDURUMU = '2' THEN 1
                                                ELSE 0 END
        ,worksite_control_estimated_progress_payment_flag =
                                                        CASE WHEN hakbil.KESINHAKDURUMU = '1' THEN 1
                                                        ELSE 0 END
        ,ROW_NUMBER() OVER (PARTITION BY proj.pspid, ekko.lifnr, zzctr_aszno ORDER BY CAST(hakedis.hakedisno as int) desc) rn
    FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_proj') }} proj
    INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekkn') }} ekkn ON proj.pspid = ekkn.gsber
    INNER JOIN (
                SELECT
                    zzctr_aszno
                    ,zzctr_haked
                    ,zzctr_resno
                    ,ebeln
                    ,lifnr
                    ,waers
                FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekko') }}
                WHERE 1=1
                    AND ZZCTR_HAKED = '1'
                    AND zzctr_aszno = zzctr_resno
                    )ekko ON ekko.ebeln = ekkn.ebeln
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hakedis') }} hakedis on ekko.ebeln = hakedis.EBELN
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hakbil') }} hakbil on ekko.ebeln = hakbil.EBELN
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa1 ON lfa1.lifnr = ekko.lifnr
    where 1=1
    )
 
,mto_order AS (
    SELECT
        ebeln
        ,hakedis_no
        ,waers
        ,onayci_id
        ,ROW_NUMBER() OVER (PARTITION BY ebeln, hakedis_no, waers ORDER BY onayci_id) AS mto_order_no
    FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_011_t_onaydr') }}
)
,previous_mto_order AS (
    SELECT
        moo.ebeln
        ,moo.hakedis_no
        ,moo.waers
        ,moo.onayci_id
    FROM mto_order moo
    LEFT JOIN mto_order mpom ON moo.ebeln = mpom.ebeln
                       AND moo.hakedis_no = mpom.hakedis_no
                       AND moo.waers = mpom.waers
                       AND moo.mto_order_no = mpom.mto_order_no - 1
    WHERE mpom.onayci_id = 'XX60'
)
,max_mto_order AS (
    SELECT
        *
    FROM (
        select
            ebeln
            ,hakedis_no
            ,waers
            ,onayci_id
            ,onayci_tanim
            ,ROW_NUMBER() OVER (PARTITION BY ebeln, hakedis_no, waers ORDER BY onayci_id desc) AS mto_order_no
        FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_011_t_onaydr') }} ) mto_order
    WHERE mto_order_no = '1'
)
 
,max_and_previous_onayci_id AS (
    SELECT
        moo.ebeln
        ,moo.hakedis_no
        ,moo.waers
        ,mmo.onayci_id AS max_onayci_id
        ,pmo.onayci_id AS previous_mto_onayci_id
    FROM (  SELECT DISTINCT
                moo.ebeln
                ,moo.hakedis_no
                ,moo.waers
            FROM mto_order moo ) AS moo
    LEFT JOIN max_mto_order mmo ON moo.ebeln = mmo.ebeln
                                AND moo.hakedis_no = mmo.hakedis_no
                                AND moo.waers = mmo.waers
    LEFT JOIN previous_mto_order pmo ON pmo.ebeln = moo.ebeln
                                    AND pmo.hakedis_no = moo.hakedis_no
                                    AND pmo.waers = moo.waers
    )
 
,current_approval AS (
    SELECT
        szl_no
        ,hkds_no
        ,waers
        ,onayci_id
        ,onayci_tnm
        ,islem
    FROM (SELECT
                szl_no,
                hkds_no,
                waers,
                onayci_id,
                onayci_tnm,
                islem,
                ROW_NUMBER() OVER (PARTITION BY szl_no, hkds_no, waers ORDER BY onayci_id DESC) AS rn
            FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_011_t_hakdet') }}) AS subquery
    WHERE rn = 1
    )

SELECT DISTINCT
	[rls_region]   = cm.RegionCode
    ,[rls_group]   = CONCAT(COALESCE(cm.KyribaGrup,''),'_',COALESCE(cm.RegionCode,''))
    ,[rls_company] = CONCAT(COALESCE(rd.company  ,''),'_'    ,COALESCE(cm.RegionCode,''),'')
    ,[rls_businessarea] = CONCAT(COALESCE(rd.project_code  ,''),'_'  ,COALESCE(cm.RegionCode,''),'')
	,rd.company
    ,rd.project_code
    ,rd.project_name
    ,cd.status
    ,cd.category AS bu_category
    ,rd.contract
	,rd.subcontractor_no
	,rd.subcontractor_name
    ,rd.estimated_progress_payment_try
    ,rd.estimated_progress_payment_usd
    ,rd.estimated_progress_payment_eur
    ,rd.completed_progress_payment_flag
    ,rd.company_preparation_flag
    ,rd.worksite_control_flag
    ,CASE
        WHEN ca.onayci_id = previous_mto_onayci_id and ca.islem = '4' and rd.progress_payment_statu = '01' THEN 1
        ELSE 0
    END AS presented_mto_flag
    ,rd.exact_progress_payment_flag
    ,rd.company_prepared_estimated_progress_payment_flag
    ,rd.worksite_control_estimated_progress_payment_flag
FROM raw_data rd
LEFT JOIN max_and_previous_onayci_id mpoi ON rd.document_no = mpoi.ebeln
                                            AND rd.progress_payment_no = TRIM(mpoi.hakedis_no)
                                            AND rd.currency = mpoi.waers
LEFT JOIN current_approval ca ON rd.document_no = ca.szl_no
                                AND rd.progress_payment_no = TRIM(ca.hkds_no)
                                AND rd.currency = ca.waers
LEFT JOIN {{ ref('dm__to_kpi_t_dim_consolidateddata') }} cd ON cd.sap_business_area = rd.project_code
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w on t001w.werks = rd.project_code
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON cm.RobiKisaKod = rd.company
WHERE rn = 1