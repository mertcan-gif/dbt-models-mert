{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
 
/*
sapdeki raporu çekerken dönem kısmı giriliyor. girilen dönem tarihinde veri var ise o tarihteki verileri
eğer yok ise girilen dönem tarihinden küçük ama en büyük tarihi baz alarak verileri getiriyor
 
 
Son Ay İşçilik Ödeme Durumu:
                            [Son Ay Hakediş (TL) + Son Ay Hakediş (USD)* kur + Son Ay Hakediş (EUR)*kur]
                            - Son Ay İşçilik Maliyet + Son Ay Ödenen Avans
    kur alınırken hakediş bitiş tarihindeki ayın 16'sı alınmalıdır. Örneğin 31.05.2024 tarihinde hakediş bitiyor ise
    16.05.2024 tarihinin kuru alınacaktır. (kümüle işçilik ödemesinde de aynısı geçerlidir.)
    . Normalde ödeme durum hesabı yukarda gibidir. Ancak iş biriminin yönlendirilmesi doğrultusunda
        bu kısımda hakedişlerden sadece tl olan hakediş tutarı kullanılacaktır.
 
Son Ay İşçilik Maliyeti: (Normal mesai adam saat + (fazla mesai saati * 1.5)) * ortalama yevmiye
    (adam saat verileri diğer hazırlamış olduğum subcontractor_sheet sorgusundan gelecek)
*/
WITH nakit as (
	SELECT 
		WERKS, 
		LIFNR, 
		EBELN, 
		HAKEDISNO, 
		ISNULL([USD], 0) AS USDPB_cash, 
		ISNULL([TRY], 0) AS TRYPB_cash, 
		ISNULL([EUR], 0) AS EURPB_cash
	FROM (
		SELECT 
			WERKS, 
			LIFNR, 
			EBELN, 
			HAKEDISNO, 
			WAERS, 
			CAST(GOITAB108 AS float) + CAST(GOITAB104 AS float) - CAST(GOITAB266 AS float) + CAST(GOITAB110 AS float) AS hesaplanan_deger
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_haktut') }}
	) AS src
	PIVOT (
		SUM(hesaplanan_deger) 
		FOR WAERS IN ([USD], [TRY], [EUR])
	) AS pvt
	)

	,tahakkuk as (	 

	SELECT 
		WERKS, 
		LIFNR, 
		EBELN, 
		HAKEDISNO, 
		ISNULL([USD], 0) AS USDPB_accrual, 
		ISNULL([TRY], 0) AS TRYPB_accrual, 
		ISNULL([EUR], 0) AS EURPB_accrual
	FROM (
		SELECT 
			WERKS, 
			LIFNR, 
			EBELN, 
			HAKEDISNO, 
			WAERS, 
			CAST(GOITAB108 AS float) + CAST(GOITAB104 AS float) AS hesaplanan_deger
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_haktut') }}
	) AS src
	PIVOT (
		SUM(hesaplanan_deger) 
		FOR WAERS IN ([USD], [TRY], [EUR])
	) AS pvt
	)

, haktut AS (
	SELECT  
		nk.WERKS,
		nk.LIFNR,
		nk.EBELN,
		nk.HAKEDISNO,
		nk.TRYPB_cash,
		nk.USDPB_cash,
		nk.EURPB_cash,
		th.TRYPB_accrual,
		th.USDPB_accrual,
		th.EURPB_accrual
	FROM nakit nk
	INNER JOIN tahakkuk th on th.WERKS = nk.WERKS
							AND th.LIFNR = nk.LIFNR
							AND th.EBELN = nk.EBELN
							AND th.HAKEDISNO = nk.HAKEDISNO
	)

,hakedis AS (
	SELECT DISTINCT
		proj.vbukr AS company,
		proj.pspid AS project_code,
		project_name = proj.post1,
		ekko.zzctr_aszno AS contract_no,
		ekko.ebeln AS sap_contract_no,
		ekko.lifnr AS subcontractor_no,
		lfa1.name1 AS subcontractor_name,
		CAST(hakedis.BASTARIH AS date) AS progress_payment_start_date,
		CAST(hakedis.BITTARIH AS date) AS progress_payment_end_date,
		hakedis.HAKEDISNO AS progress_payment_no,
		CAST(haktut.TRYPB_accrual AS money) AS last_month_progress_payment_try_accrual,
		CAST(haktut.USDPB_accrual AS money) AS last_month_progress_payment_usd_accrual,
		CAST(haktut.EURPB_accrual AS money) AS last_month_progress_payment_eur_accrual,
		CAST(haktut.TRYPB_cash AS money) AS last_month_progress_payment_try_cash,
		CAST(haktut.USDPB_cash AS money) AS last_month_progress_payment_usd_cash,
		CAST(haktut.EURPB_cash AS money) AS last_month_progress_payment_eur_cash,
		hakbil.ORTALAMAYEVMIYE AS avg_daily_wage
		,ROW_NUMBER() OVER (PARTITION BY proj.pspid, ekko.ebeln, ekko.lifnr, hakedis.HAKEDISNO ORDER BY CAST(hakedis.BITTARIH AS date)) rn
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_proj') }} proj
	LEFT JOIN haktut ON proj.pspid = haktut.werks
	INNER JOIN (
		SELECT
			zzctr_aszno,
			ebeln,
			lifnr,
			bstyp
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekko') }}
		WHERE ZZCTR_HAKED = '1'
			AND zzctr_aszno = zzctr_resno) ekko ON ekko.ebeln = haktut.EBELN
	INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hakedis') }} hakedis ON haktut.ebeln = hakedis.EBELN 
																		AND haktut.HAKEDISNO = hakedis.HAKEDISNO
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hakbil') }} hakbil ON ekko.ebeln = hakbil.EBELN
																		AND ekko.lifnr = hakbil.LIFNR
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa1 ON lfa1.lifnr = ekko.lifnr
    )
 
,raw_data AS (
    SELECT
        project_code = proj.pspid
        ,project_name = proj.post1
        ,contract = zzctr_aszno
        ,document_no = ekko.ebeln
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
        ,performance_guarantee = hakedis.KESINTEMINAT
        ,company_prepared_estimated_progress_payment_flag =  
                                                CASE WHEN hakbil.KESINHAKDURUMU = '2' THEN 1
                                                ELSE 0 END
        ,worksite_control_estimated_progress_payment_flag =
                                                        CASE WHEN hakbil.KESINHAKDURUMU = '1' THEN 1
                                                        ELSE 0 END
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_proj') }} proj
	LEFT JOIN haktut ON proj.pspid = haktut.werks
	INNER JOIN (
		SELECT
			zzctr_aszno,
			ebeln,
			lifnr,
			bstyp,
			waers
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekko') }}
		WHERE ZZCTR_HAKED = '1'
			AND zzctr_aszno = zzctr_resno) ekko ON ekko.ebeln = haktut.EBELN
	INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hakedis') }} hakedis ON haktut.ebeln = hakedis.EBELN 
																		AND haktut.HAKEDISNO = hakedis.HAKEDISNO
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hakbil') }} hakbil ON ekko.ebeln = hakbil.EBELN
																		AND ekko.lifnr = hakbil.LIFNR
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
,kalan_hakedisler AS (
SELECT DISTINCT
    rd.project_code
    ,rd.project_name
    ,cd.status
    ,cd.category AS bu_category
    ,rd.contract
    ,rd.document_no
    ,rd.progress_payment_no
    ,rd.progress_payment_start_date
    ,rd.progress_payment_end_date
    --,rd.currency
    ,rd.performance_guarantee
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
    )
,currency_table AS (
    SELECT
        FORMAT(TRY.date_value, 'yyyy-MM') as date,
        TRY.try_value AS try_rate,
        USD.try_value AS usd_to_try_rate,
        EUR.try_value AS eur_to_try_rate
    FROM {{ ref('dm__dimensions_t_dim_dailys4currencies') }} AS TRY
        JOIN {{ ref('dm__dimensions_t_dim_dailys4currencies') }} AS USD ON TRY.date_value = USD.date_value AND USD.currency = 'USD'
        JOIN {{ ref('dm__dimensions_t_dim_dailys4currencies') }} AS EUR ON TRY.date_value = EUR.date_value AND EUR.currency = 'EUR'
    WHERE
        TRY.currency = 'TRY'
        AND DAY(TRY.date_value) = '16'
)
 
SELECT
    [rls_region]   = cm.RegionCode
    ,[rls_group]   = CONCAT(COALESCE(cm.KyribaGrup,''),'_',COALESCE(cm.RegionCode,''))
    ,[rls_company] = CONCAT(COALESCE(h.company  ,''),'_'    ,COALESCE(cm.RegionCode,''),'')
    ,[rls_businessarea] = CONCAT(COALESCE(h.project_code  ,''),'_'  ,COALESCE(cm.RegionCode,''),'')
    ,h.*
    ,kh.status
    ,kh.bu_category
    /*
        Aynı sözleşmedeki son hakediş tamamlandıysa veya kesinleştiyse önceki hakedişlerin de
        tamamlanmış veya kesinleşmiş olarak göstermek için aşağıdaki case when eklenmiştir.
    */
    ,CASE  
        WHEN h.progress_payment_no <= ( select max(progress_payment_no) from kalan_hakedisler kh2
                                            where kh2.completed_progress_payment_flag = '1'
                                            and kh2.contract = kh.contract
                                            and kh2.project_code = kh.project_code) THEN 1
    ELSE 0
    END AS completed_progress_payment_flag
    ,CASE  
        WHEN h.progress_payment_no <= (select max(progress_payment_no) from kalan_hakedisler kh2
                                            where 1=1
                                            and kh2.exact_progress_payment_flag = '1'
                                            and kh2.contract = kh.contract
                                            and kh2.project_code = kh.project_code ) THEN 1
        ELSE 0
    END AS exact_progress_payment_flag
    ,kh.company_preparation_flag
    ,kh.worksite_control_flag
    ,kh.presented_mto_flag
    ,kh.estimated_progress_payment_try
    ,kh.estimated_progress_payment_usd
    ,kh.estimated_progress_payment_eur
    ,kh.performance_guarantee
    ,kh.company_prepared_estimated_progress_payment_flag
    ,kh.worksite_control_estimated_progress_payment_flag
    ,h.last_month_progress_payment_try_cash + h.last_month_progress_payment_usd_cash * ct.usd_to_try_rate + h.last_month_progress_payment_eur_cash * ct.eur_to_try_rate AS total_progress_payment_tl
    ,(h.last_month_progress_payment_try_cash / ct.usd_to_try_rate) + h.last_month_progress_payment_usd_cash + (h.last_month_progress_payment_eur_cash * ct.eur_to_try_rate / ct.usd_to_try_rate) AS total_progress_payment_usd
    ,(h.last_month_progress_payment_try_cash / ct.eur_to_try_rate) + (h.last_month_progress_payment_usd_cash * ct.usd_to_try_rate / ct.eur_to_try_rate) + h.last_month_progress_payment_eur_cash AS total_progress_payment_eur
FROM hakedis h
LEFT JOIN kalan_hakedisler kh ON kh.project_code = h.project_code
                            AND kh.document_no = h.sap_contract_no
                            AND kh.progress_payment_no = h.progress_payment_no
LEFT JOIN currency_table ct ON FORMAT(kh.progress_payment_end_date, 'yyyy-MM') = ct.date
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON cm.RobiKisaKod = h.company
where 1=1