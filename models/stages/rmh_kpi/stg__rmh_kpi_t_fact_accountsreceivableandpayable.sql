{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
WITH RAW_CTE AS
(
    /*
    159lu hesabın alacaklara yazılmasının sebebi external firmaya verilmiş avansı alacak olarak gösterebilmek içindir.
    */
    SELECT
        company = CASE 
                        WHEN (LEN(ACDOCA.KUNNR) = 3 OR LEN(ACDOCA.LIFNR) = 3) AND LEFT(RACCT,3) IN ('120','133','136') THEN ACDOCA.KUNNR
                        WHEN (LEN(ACDOCA.KUNNR) = 3 OR LEN(ACDOCA.LIFNR) = 3) AND LEFT(RACCT,3) IN ('320','326','331','336','333','159') THEN ACDOCA.LIFNR
                        WHEN (LEN(ACDOCA.KUNNR) <> 3 AND LEN(ACDOCA.LIFNR) <> 3) AND LEFT(RACCT,3) IN ('120','133','136') THEN KNA1.NAME1
                        WHEN (LEN(ACDOCA.KUNNR) <> 3 AND LEN(ACDOCA.LIFNR) <> 3) AND LEFT(RACCT,3) IN ('320','326','331','336','333','159') THEN LFA1.NAME1
                    END, 
        main_company = ACDOCA.RBUKRS,
        business_area = ACDOCA.RBUSA, 
        business_area_description = T001W.NAME1, 
        document_number = ACDOCA.BELNR,
        document_line_item = ACDOCA.BUZEI,
        fiscal_year = ACDOCA.GJAHR,
        document_type = ACDOCA.BLART,
        posting_date = ACDOCA.BUDAT,
        document_date = ACDOCA.BLDAT,
        entry_date = BKPF.CPUDT,
        main_account = LEFT(RACCT,3), 
        clearing_document_no = ACDOCA.AUGBL,
        account_number_last_3_digits = RIGHT(RACCT,3), 
		in_group_flag = CASE 
							WHEN (LEN(ACDOCA.KUNNR) = 3 OR LEN(ACDOCA.LIFNR) = 3) THEN 'In Group'
							ELSE 'Out Group'
						END,
		account_type = CASE 
							WHEN LEFT(RACCT,3) IN ('120','133','136') THEN 'Receivable'
							WHEN LEFT(RACCT,3) IN ('320','326','331','336','333','159') THEN 'Payable'
						END,
		customer_vendor_code = CASE 
									WHEN LEFT(RACCT,3) IN ('120','133','136') THEN ACDOCA.KUNNR
									WHEN LEFT(RACCT,3) IN ('320','326','331','336','333','159') THEN ACDOCA.LIFNR
								END,
        customer_vendor_name = CASE 
									WHEN LEFT(RACCT,3) IN ('120','133','136') THEN KNA1.NAME1
									WHEN LEFT(RACCT,3) IN ('320','326','331','336','333','159') THEN LFA1.NAME1
								END,
        document_currency = RWCUR, 
        amount_in_document_currency = CASE
										  WHEN TCURX.CURRDEC = 3 THEN WSL/10
									  ELSE WSL END,
        amount_in_company_currency = HSL,
        due_date = CASE WHEN NETDT <> 00000000 THEN CAST(NETDT AS DATE) ELSE NULL END
    FROM {{ ref('stg__s4hana_t_sap_acdoca') }} AS ACDOCA
        LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} AS KNA1 ON ACDOCA.KUNNR = KNA1.KUNNR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} AS LFA1 ON ACDOCA.LIFNR = LFA1.LIFNR
        LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} AS T001W ON ACDOCA.RBUSA = T001W.WERKS
        LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} AS BKPF ON ACDOCA.BELNR = BKPF.BELNR
                                                                                AND ACDOCA.RBUKRS = BKPF.BUKRS
                                                                                AND ACDOCA.GJAHR = BKPF.GJAHR
        LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} AS TCURX ON ACDOCA.RWCUR = TCURX.CURRKEY
        LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }}  kuc ON ACDOCA.RBUKRS = kuc.RobiKisaKod
    WHERE 1=1
        AND ACDOCA.rbukrs = 'RMH'
		AND LEFT(racct,3) IN (
						'120',
						'133',
						'136',
                        '159',
						'320',
						'326',
						'331',
						'333',
						'336'
						)
        AND BKPF.STBLG <> 'X'  -- nwc ar sorgusunda bu filtre var ancak nwc ap sorgusunda yok. Göker Bey'e sorulmalı.
		AND RIGHT(racct,2) <> '40'
		AND RIGHT(racct,2) <> '60'
		AND NOT (ACDOCA.kunnr = '' AND ACDOCA.lifnr = '')
    )
 
SELECT
    [rls_region]   = kuc_rls.RegionCode
    ,[rls_group]   = CONCAT(COALESCE(kuc_rls.KyribaGrup,''),'_',COALESCE(kuc_rls.RegionCode,''))
    ,[rls_company] = CONCAT(COALESCE(kuc_rls.RobiKisaKod  ,''),'_'  ,COALESCE(kuc_rls.RegionCode,''),'')
    ,[rls_businessarea] = CONCAT(COALESCE(RAW_CTE.business_area  ,''),'_'   ,COALESCE(kuc_rls.RegionCode,''),'')
    ,[group] = CASE 
                    WHEN kuc_group.KyribaGrup IS NOT NULL THEN kuc_group.KyribaGrup 
                    ELSE 'External' 
                END
    ,RAW_CTE.company
    ,RAW_CTE.business_area
    ,RAW_CTE.business_area_description
    ,RAW_CTE.document_number
    ,RAW_CTE.document_line_item
    ,RAW_CTE.fiscal_year
    ,RAW_CTE.document_type
    ,RAW_CTE.posting_date
    ,RAW_CTE.document_date
    ,RAW_CTE.entry_date
    ,RAW_CTE.clearing_document_no
    ,RAW_CTE.main_account
    ,RAW_CTE.account_number_last_3_digits
    ,RAW_CTE.in_group_flag
    ,RAW_CTE.account_type
    ,RAW_CTE.customer_vendor_code
    ,RAW_CTE.customer_vendor_name
    ,RAW_CTE.document_currency
    ,RAW_CTE.amount_in_document_currency
    ,RAW_CTE.amount_in_company_currency
    ,RAW_CTE.due_date
FROM RAW_CTE
    LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc_group ON RAW_CTE.company = kuc_group.RobiKisaKod
    LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc_rls ON kuc_rls.RobiKisaKod = RAW_CTE.main_company
WHERE NOT ((LEN(RAW_CTE.customer_vendor_code) = 3) AND RAW_CTE.main_account = '159')
