{{
  config(
    materialized = 'table',tags = ['fms_kpi']
    )
}}

WITH vehicle_2 AS (
    SELECT 
		license_plate
		,supply_type
		,ROW_NUMBER() OVER (PARTITION BY license_plate ORDER BY reporting_date desc) rn
    FROM {{ ref('dm__fms_kpi_t_dim_vehicles') }}
)

,vehicle_dedup AS (
SELECT 
	*
FROM vehicle_2
WHERE rn = 1
)
 
,expenses AS (
    SELECT 
		[rls_region] = cm.RegionCode,
		rls_group = cm.KyribaGrup + '_' + cm.RegionCode,
		rls_company = acd.rbukrs + '_' + cm.RegionCode,
		[rls_businessarea] = acd.rbusa+ '_' + cm.RegionCode,
        vehicle_dedup.supply_type,
        acd.RBUKRS as company,
        acd.RBUSA as business_area,
        acd.budat as documantation_date,
        vehicle_dedup.license_plate,
        acd.RWCUR as currency,
        acd.WSL as cost_wsl,
        acd.HSL as cost_hsl,
        CASE
            WHEN acd.RACCT    = '7408000003' THEN 'Amortisman'
            WHEN acd.RACCT IN ('7401208000') THEN 'Rent'
            WHEN acd.RACCT LIKE '7__1211011' THEN 'Tax Traffic'
            WHEN acd.RACCT LIKE '7__1211012' THEN 'Tax Kasko'
            WHEN acd.RACCT IN ('7401211013') THEN 'Insurance License'
            WHEN acd.RACCT IN ('7401208004') THEN 'Other Expense'
            WHEN acd.RACCT IN ('7401208005') THEN 'Traffic Fine'
            WHEN acd.RACCT IN ('7401208003') THEN 'Maintenance Expense'
			WHEN acd.RACCT IN ('7401209001') THEN 'Vehicle Tax'
			WHEN acd.RACCT IN ('7401211020') THEN 'Insurance'
			WHEN acd.RACCT IN ('6890101003') THEN 'Vehicle Sales Expense'
            WHEN acd.RACCT IN ('7401208001') THEN 'Fuel'
            WHEN acd.RACCT IN ('7401209000') THEN 'Stamp Tax'
            WHEN acd.RACCT IN ('7401209008') THEN 'Notary Fees'
            WHEN acd.RACCT IN ('7401212007') THEN 'Vehicle Tracking Device'
        END as category
    FROM {{ ref('stg__s4hana_t_sap_acdoca') }} acd
        LEFT JOIN vehicle_dedup on vehicle_dedup.license_plate collate database_default = substring(acd.AUFNR,2,(LEN(acd.AUFNR)))  collate database_default
		LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm on acd.rbukrs = cm.RobiKisaKod
		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf ON acd.rbukrs = bkpf.bukrs
																AND acd.belnr = bkpf.belnr
																AND acd.gjahr = bkpf.gjahr
    WHERE 1=1
		AND bkpf.xreversing = 0
        AND bkpf.xreversed = 0  
        AND 
            (acd.RACCT IN   ('7408000003' 
                            ,'7401208000'
                            ,'7401211013'
                            ,'7401208004'
                            ,'7401208005'
                            ,'7401208003'	
							,'7401209001'
							,'7401211020'
							,'6890101003'
                            ,'7401208001'
                            ,'7401209000'
                            ,'7401209008'
                            ,'7401212007')
            OR acd.RACCT LIKE '7__1211011'
            OR acd.RACCT LIKE '7__1211012')
        AND acd.blart <> 'SA'
)
SELECT
*
FROM expenses
WHERE license_plate IS NOT NULL