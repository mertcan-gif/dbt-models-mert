{{
  config(
    materialized = 'view',tags = ['co_kpi']
    )
}}
WITH date_ranges AS (
    SELECT 
        pspid,
        TRY_CONVERT(DATE, REPLACE(sprog, '.', '-')) AS StartDate,
        TRY_CONVERT(DATE, REPLACE(eprog, '.', '-')) AS EndDate
    FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_proj') }} 
),
year_list AS (
    SELECT 
        pspid,
        YEAR(StartDate) AS [Year],
        StartDate,
        EndDate
    FROM date_ranges
    WHERE StartDate IS NOT NULL AND EndDate IS NOT NULL
		
    UNION ALL

    SELECT 
        pspid,
        [Year] + 1,
        StartDate,
        EndDate
    FROM year_list
    WHERE [Year] < YEAR(EndDate)
),
project_start as (
SELECT 
    pspid,
    CASE 
        WHEN [Year] < 2000 THEN 0
        ELSE [Year]
    END AS [year],
    StartDate as [start_date],
    EndDate  as [end_date]
FROM year_list
),
cte_raw AS (
SELECT 
	distinct
	fincode AS project
	,versi AS [buku_version]
	,ps.[year] AS fiscal_fm_fund
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfm_001_t_ver_nr') }} ver
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tkvs') }} tkvs ON ver.mandt = tkvs.mandt 
	LEFT JOIN  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfm_001_t_onylog') }} onylog ON ver.mandt = onylog.mandt
	LEFT JOIN project_start ps ON ver.fincode = ps.pspid
WHERE versi like 'Q%'

	UNION ALL

SELECT 
	distinct
	fincode AS project
	,versi AS [buku_version]
	,MIN(ps.[year]) AS fiscal_fm_fund
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfm_001_t_ver_nr') }}  ver
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tkvs') }} tkvs ON ver.mandt = tkvs.mandt 
	LEFT JOIN  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfm_001_t_onylog') }} onylog ON ver.mandt = onylog.mandt
	LEFT JOIN project_start ps ON ver.fincode = ps.pspid
WHERE versi like 'R%'
	or versi like 'T%'
GROUP BY 
	fincode,
	versi
),
app_log AS (
    SELECT 
        l.gjahr,
        l.buku_version,
        l.fm_fund,
        l.onayci_id,
        ROW_NUMBER() OVER (PARTITION BY l.gjahr, l.buku_version, l.fm_fund ORDER BY l.onayci_id DESC) AS rn
    FROM 
        {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfm_001_t_onylog') }} l 
),
app_desc AS (
SELECT 
distinct
    l.gjahr,
    l.buku_version,
    l.fm_fund,
    o2.onayci_id,
    o2.grup_id 
FROM 
    app_log l
JOIN 
    {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfm_001_t_grup') }} o1 ON l.onayci_id = o1.onayci_id
LEFT JOIN 
    {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfm_001_t_grup') }} o2 ON l.fm_fund = o2.fm_fund AND o2.onayci_id > l.onayci_id
WHERE 
    l.rn = 1
    AND o2.onayci_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 
        FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfm_001_t_onylog') }} l2 
        WHERE l2.gjahr = l.gjahr 
          AND l2.buku_version = l.buku_version 
          AND l2.fm_fund = l.fm_fund 
          AND l2.onayci_id = o2.onayci_id
    )
),
approver AS (
SELECT
la.*
,ROW_NUMBER() over(partition by gjahr, buku_version, fm_fund order by onayci_id) rn
FROM app_desc la

),
last_approver AS(
SELECT *
FROM approver
WHERE rn = 1
),
project_company_mapping AS (
SELECT
	name1
	,WERKS
	,w.BWKEY
	,bukrs
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} w
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} k ON w.bwkey = k.bwkey
),
final AS (
SELECT
	dim_comp.rls_region
	,dim_comp.rls_group
	,dim_comp.rls_company
	,rls_businessarea = CONCAT(c.project , '_' , dim_comp.rls_region)
	,m.bukrs as company
	,project
	,m.name1 as project_name
	,[buku_version] 
	,fiscal_fm_fund
	,[status] = 
			CASE 
				WHEN NOT EXISTS (
					SELECT
					bukrs
					,fm_fund
					,gjahr
					,buku_version
					,onayci_id
					FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfm_001_t_onylog') }} onlylog
					WHERE 1=1
						AND c.project = onlylog.fm_fund
						AND c.[buku_version] = onlylog.buku_version
						AND c.fiscal_fm_fund = onlylog.gjahr
						AND onlylog.seviye = '4'
				) THEN 'Baslamadi'
				
				ELSE (
					CASE
						WHEN EXISTS (
							SELECT
							bukrs
							,fm_fund
							,gjahr
							,buku_version
							FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfm_001_t_log_v2') }} v
							WHERE 1=1
								AND c.project = v.fm_fund
								AND c.[buku_version] = v.buku_version
								AND c.fiscal_fm_fund = v.gjahr
								AND fdocumentnumber <> ''
						) THEN 'Tamamlandı'
			
						ELSE (
							CASE
								WHEN EXISTS (
									SELECT
									bukrs
									,gjahr
									,buku_version
									FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfm_001_t_onylog') }} onylog
									WHERE 1=1
										AND c.project = onylog.fm_fund
										AND c.[buku_version] = onylog.buku_version
										AND c.fiscal_fm_fund = onylog.gjahr
										AND onayci_id <> ''
								) THEN 'Onay Sürecinde'

								ELSE 'Girişler Devam Ediyor'
							END
						)
					END
				)
			END

FROM cte_raw c
	LEFT JOIN project_company_mapping m ON c.project = m.werks
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON m.bukrs = dim_comp.RobiKisaKod
),
approver_name AS (
SELECT
	distinct
	CONCAT(adrp.name_first, ' ', adrp.name_last) as conc_name
	,ust.von
	,la.gjahr
	,la.buku_version
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_usgrp_user') }} usgrp 
INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_agr_users') }} agr ON usgrp.bname = agr.uname 
INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_usr21') }} usr ON agr.uname = usr.bname 
INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_adrp') }} adrp ON usr.persnumber = adrp.persnumber 
INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_agr_1251') }} agr_2 ON agr_2.agr_name = agr.agr_name 
													 AND agr_2.[object] = N'ZPS_B_WERK'
													 AND agr_2.field = N'WERKS'
INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ust12') }} ust ON ust.auth = agr_2.auth 
												AND ust.objct = N'ZPS_B_WERK'
INNER JOIN last_approver la ON ust.von = la.fm_fund
WHERE usgrp.usergroup = la.grup_id

),
approver_full_name as (
SELECT
	STRING_AGG(conc_name , ' /') as full_name
	,von
	,gjahr
	,buku_version
FROM approver_name
GROUP BY
	von
	,gjahr
	,buku_version
)
SELECT 
distinct
f.*
,budget_approval_group =
		CASE
			WHEN f.[status] = N'Onay Sürecinde' THEN la.grup_id
			ELSE NULL
		END
,approver_definition =
		CASE
			WHEN f.[status] = N'Onay Sürecinde' THEN ony.onayci_tanim
			ELSE NULL
		END
,approver =
		CASE
			WHEN f.[status] = N'Onay Sürecinde' THEN an.full_name
			ELSE NULL
		END
,sprog as f_start_date
,eprog as f_end_date
FROM final f
LEFT JOIN last_approver la ON f.project = la.fm_fund
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfm_001_t_onayci') }} ony ON la.onayci_id = ony.onayci_id 
LEFT JOIN approver_full_name an ON f.project = an.von
INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_proj') }} proj ON f.project = proj.pspid 
