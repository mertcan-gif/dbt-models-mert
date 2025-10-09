{{
  config(
    materialized = 'table',tags = ['eff_kpi']
    )
}}

WITH cte_bsik AS (
SELECT 
		rbukrs
		,lifnr
		,racct
		,blart
		,rbusa
		,rwcur
		,augdt
		,wsl = cast(wsl as money)
		,hsl = cast(hsl as money)
		,budat
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hareketsizcari') }}
WHERE 1=1

		--AND (augdt = '' OR convert(date, augdt, 112) > DATEADD(DAY, -1, CAST(GETDATE() as date)))
		AND bstat = ''
		AND koart = 'K'
		AND rbukrs IN (
			'BAL'
			,'BHC'
			,'BNA'
			,'BNR'
			,'HCA'
			,'REC'
			,'RIA'
			,'RKZ'
			,'RMG'
			,'RMI'
			,'VOL'
			,'VYG'
		)
		and vorgn in ('AS91','AZAF','AZBU','AZUM','GLYC','HRP1','RFBU',
            'RFST','RMBL','RMRP','RMWA','RMWE','RMWL','SD00',
            'UMAI','UMZI','ZUGA','ABGA','ACEA')
),

cte_bsik_neg as (
select *
from cte_bsik
where (augdt = '' OR convert(date, augdt, 112) > DATEADD(DAY, -1, CAST(GETDATE() as date)))

),
cte_bsid AS (
SELECT 
		rbukrs
		,kunnr
		,racct
		,blart
		,rbusa
		,rwcur
		,augdt
		,wsl = cast(wsl as money)
		,hsl = cast(hsl as money)
		,budat
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hareketsizcari') }}
WHERE 1=1
		--AND (augdt = '' OR convert(date, augdt, 112) > DATEADD(DAY, -1, CAST(GETDATE() as date)))
		AND bstat = ''
		AND koart = 'D'
		--augdt için seçili tarihten büyük olanlar gelecek şekilde filtre isteniyor.
		--AND kunnr <> ''

				AND rbukrs IN (
			'BAL'
			,'BHC'
			,'BNA'
			,'BNR'
			,'HCA'
			,'REC'
			,'RIA'
			,'RKZ'
			,'RMG'
			,'RMI'
			,'VOL'
			,'VYG'
		)
		and vorgn in ('AS91','AZAF','AZBU','AZUM','GLYC','HRP1','RFBU',
            'RFST','RMBL','RMRP','RMWA','RMWE','RMWL','SD00',
            'UMAI','UMZI','ZUGA','ABGA','ACEA')
),

cte_bsid_neg as (
select *
from cte_bsid 
where (augdt = '' OR convert(date, augdt, 112) > DATEADD(DAY, -1, CAST(GETDATE() as date)))

),

cte_main AS ( 
SELECT 
		com.KyribaGrup
		,bk.rbukrs
		,bk.lifnr
		,bk.rbusa
		,lf.name1
		,bk.rwcur
		,bk.blart
		,bk.wsl
		,bk.hsl
		,CONVERT(DATE, bk.budat, 104) AS budat
		,lf.ktokk
		,bk.racct
FROM cte_bsik bk
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lf ON lf.lifnr = bk.lifnr
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} com ON com.RobiKisaKod = bk.rbukrs 
WHERE 1=1
		AND LEFT (bk.racct, 1) <> '9'
		AND CONVERT(DATE, bk.budat, 104) > '2019-01-01'

UNION ALL

SELECT
		com.KyribaGrup
		,bs.rbukrs
		,bs.kunnr
		,bs.rbusa
		,lf.name1
		,bs.rwcur
		,bs.blart
		,bs.wsl
		,bs.hsl
		,CONVERT(DATE, bs.budat, 104) AS BUDAT
		,lf.ktokk
		,bs.racct
FROM cte_bsid bs 
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lf ON lf.lifnr = bs.kunnr
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} com ON com.RobiKisaKod = bs.rbukrs
	WHERE 1=1
		AND LEFT (bs.racct, 1) <> '9'
		AND CONVERT(DATE, bs.budat, 104) > '2019-01-01'

),

cte_main_neg AS ( 
SELECT 
		com.KyribaGrup
		,bk.rbukrs
		,bk.lifnr
		,bk.rbusa
		,lf.name1
		,bk.rwcur
		,bk.blart
		,bk.wsl
		,bk.hsl
		,CONVERT(DATE, bk.budat, 104) AS budat
		,lf.ktokk
		,bk.racct
FROM cte_bsik_neg bk
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lf ON lf.lifnr = bk.lifnr
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} com ON com.RobiKisaKod = bk.rbukrs 
WHERE 1=1
		AND LEFT (bk.racct, 1) <> '9'
		AND CONVERT(DATE, bk.budat, 104) > '2019-01-01'

UNION ALL

SELECT
		com.KyribaGrup
		,bs.rbukrs
		,bs.kunnr
		,bs.rbusa
		,lf.name1
		,bs.rwcur
		,bs.blart
		,bs.wsl
		,bs.hsl
		,CONVERT(DATE, bs.budat, 104) AS BUDAT
		,lf.ktokk
		,bs.racct
FROM cte_bsid_neg bs 
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lf ON lf.lifnr = bs.kunnr
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} com ON com.RobiKisaKod = bs.rbukrs
	WHERE 1=1
		AND LEFT (bs.racct, 1) <> '9'
		AND CONVERT(DATE, bs.budat, 104) > '2019-01-01'

),
cte_grouped AS (
SELECT 
		c.rbukrs
		,c.lifnr
		,c.name1
		,c.rbusa
		,c.racct
		,c.blart
		,SUM(c.hsl) AS AmountLC
		,SUM(c.wsl) AS Amount
		,MAX(c.budat) AS Son_Hareket_Tarihi
		,MIN(c.budat) AS Ilk_Hareket_Tarihi
		--,first_negative_date =	
		--		CASE
		--			WHEN SUM(c.wsl) > 0 THEN MIN(c.budat)
		--			ELSE null
		--		END
		--,DATEDIFF(day, MAX(c.budat), CONVERT(DATE, GETDATE(), 104)) AS gun_farki
		-- ,DATEDIFF(day, MIN(c.budat), CONVERT(DATE, GETDATE(), 104)) AS negatif_gun_farki
		,MAX(MAX(c.budat)) OVER (PARTITION BY lifnr, name1, rbusa) AS gun_farki_by_rbusa
FROM cte_main c
WHERE c.lifnr <> ''
GROUP BY 
	c.rbukrs
	,c.lifnr
	,c.name1
	,c.rbusa
	,c.racct
	,c.blart
),

cte_grouped_neg as (
SELECT 
		c.rbukrs
		,c.lifnr
		,c.name1
		,c.rbusa
		,c.racct
		,c.blart
		,first_negative_date =	
				CASE
					WHEN SUM(c.wsl) > 0 THEN MIN(c.budat)
					ELSE null
				END
		,MAX(MAX(c.budat)) OVER (PARTITION BY lifnr, name1, rbusa) AS gun_farki_by_rbusa
FROM cte_main_neg c
WHERE c.lifnr <> ''
GROUP BY 
	c.rbukrs
	,c.lifnr
	,c.name1
	,c.rbusa
	,c.racct
	,c.blart
),

neg_day_diff AS (
SELECT 
    rbusa,
    rbukrs,
    lifnr,
    DATEDIFF(DAY, 
             (SELECT MIN(first_negative_date) 
              FROM cte_grouped_neg AS f2 
              WHERE f2.rbusa = fact.rbusa 
              AND f2.rbukrs = fact.rbukrs 
              AND f2.lifnr = fact.lifnr), 
             GETDATE()) AS negatif_gun_farki
FROM cte_grouped_neg fact
GROUP BY fact.rbusa, fact.rbukrs, fact.lifnr

),

cte as (
select 
    CASE 
        WHEN ISNUMERIC([lifnr]) = 1 THEN SUBSTRING([lifnr], 4, LEN([lifnr]) - 3) 
        ELSE [lifnr] 
    END AS vendor
,bukrs
--,gsber
-- ,max(cast(zzperf_guarantee_cek_senet as float)) zzperf_guarantee_cek_senet
-- ,max(cast(zzperf_guarantee as float)) zzperf_guarantee
,max(cast(zzpossible_risk_amount as float)) zzpossible_risk_amount 
,max(cast(zzenforce_amount as float)) zzenforce_amount 
,max(cast(zzcases_number as float)) zzcases_number
,max(cast(zzenforce_number as float)) zzenforce_number
,max(cast(zzmediations_number as float)) zzmediations_number
from "aws_stage"."s4_odata"."raw__s4hana_t_sap_zfi_001_t_tsrisk"
GROUP BY
bukrs,
lifnr
),

cte_tm as (
select 
    CASE 
        WHEN ISNUMERIC([lifnr]) = 1 THEN SUBSTRING([lifnr], 4, LEN([lifnr]) - 3) 
        ELSE [lifnr] 
    END AS vendor
,bukrs
,gsber
,max(cast(zzperf_guarantee_cek_senet as float)) zzperf_guarantee_cek_senet
,max(cast(zzperf_guarantee as float)) zzperf_guarantee
,max(cast(zzbehalf_pay_num as float)) zzbehalf_pay_num
,max(cast(zzbehalf_pay_diger as float)) zzbehalf_pay_diger
from "aws_stage"."s4_odata"."raw__s4hana_t_sap_zfi_001_t_tsrisk"
GROUP BY
bukrs,
lifnr,
gsber
),

total_amount_bukrs AS (
SELECT 
	rbukrs
	,lifnr
	,SUM(AmountLC) AS total_amount_company
FROM cte_grouped g
GROUP BY
	rbukrs,
	lifnr
),

cte_final AS (

SELECT
		lifnr
		,rbukrs
		,budat
		,SUM(hsl) AS Amount_by_bukrs
FROM cte_main
WHERE lifnr <> ''
GROUP BY 
	lifnr
	,rbukrs
	,budat
),

cte_end as (
SELECT 
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea = CONCAT(cg.rbusa, '_' , rls_region)
		,cg.rbukrs as [company]
		,cg.lifnr as 'vendor_code'
		,cg.name1 as [name]
		,cg.rbusa as 'business_area'
		,cg.blart as 'document_type'
		,cg.AmountLC as 'amount_lc'
		,cg.Amount as [amount]
		,final_guarantee_amount =
						CASE
							WHEN zzperf_guarantee_cek_senet is not null and zzperf_guarantee is not null THEN (cast(zzperf_guarantee_cek_senet as float) * (0.5) + cast(zzperf_guarantee as float))
							ELSE 0
						END
		,debt_balance = 
						CASE
							WHEN tb.total_amount_company <> 0 THEN cg.AmountLC + ((CAST(zzpossible_risk_amount as float) + CAST(zzenforce_amount as float)) * 1.25) * cg.AmountLC / tb.total_amount_company
							ELSE cg.AmountLC
						END
		,cg.Son_Hareket_Tarihi as 'last_transaction_date'
		,cg.Ilk_Hareket_Tarihi as 'first_transaction_date'
		,cgn.first_negative_date
		--,cg.gun_farki AS Gun_Farki
		,ng.negatif_gun_farki
		--,cf.Amount_by_bukrs as 'amount_by_company'
		,tb.total_amount_company as amount_by_company
		,cg.racct AS 'account_number'
		,zzbehalf_pay_num
		,zzbehalf_pay_diger
		,zzcases_number
		,zzenforce_number
		,zzmediations_number
FROM cte_grouped cg
LEFT JOIN cte_grouped_neg cgn ON cg.rbukrs = cgn.rbukrs
		AND cg.lifnr = cgn.lifnr
		AND cg.rbusa = cgn.rbusa
		AND cg.racct = cgn.racct
		AND cg.blart = cgn.blart
LEFT JOIN neg_day_diff ng ON cg.rbukrs = ng.rbukrs
						  AND cg.rbusa = ng.rbusa
						  AND cg.lifnr = ng.lifnr
LEFT JOIN cte_final cf ON cg.rbukrs = cf.rbukrs
		AND cg.lifnr = cf.lifnr
		AND cg.Son_Hareket_Tarihi = cf.budat
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON cg.rbukrs = dim_comp.RobiKisaKod
LEFT JOIN cte ts ON cg.rbukrs = ts.bukrs
				 --AND cg.rbusa = ts.gsber
				 AND cg.lifnr = ts.vendor
LEFT JOIN cte_tm tm ON cg.rbukrs = tm.bukrs
					AND cg.rbusa = tm.gsber
					AND cg.lifnr = tm.vendor
LEFT JOIN total_amount_bukrs tb ON cg.rbukrs = tb.rbukrs
								AND cg.lifnr = tb.lifnr
)

SELECT
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,[company]
	,vendor_code
	,[name]
	,business_area
	,document_type
	,amount_lc
	,[amount]
	,final_guarantee_amount
	,debt_balance
	,last_transaction_date
	,first_transaction_date
	,first_negative_date
	,amount_by_company
	,account_number
	,class =
		CASE
			WHEN cte_end.negatif_gun_farki >= 90 THEN
			(
				CASE
					WHEN LEFT(vendor_code, 1) IN ('1', '2') THEN
					(
						CASE
							WHEN ((SUM(debt_balance) OVER(partition by vendor_code, [company], business_area) * (-1)) + final_guarantee_amount) < 0 THEN
							(
								CASE
									WHEN cast(zzbehalf_pay_num as float) > 3 OR cast(zzbehalf_pay_diger as float) > 50000 THEN 
									(
										CASE
											WHEN cast(zzcases_number as float) >= 1 OR cast(zzenforce_number as float) >= 1 OR CAST(zzmediations_number as float) >= 1 THEN 'Çok Yüksek Riskli'
											ELSE 'Yüksek Riskli'
										END
									)
									ELSE 'Riskli'
									END
							)
						ELSE NULL
						END
					)
				ELSE NULL
				END
			)
		ELSE NULL
		END
FROM cte_end
