{{
  config(
    materialized = 'table',tags = ['gyg_kpi','realizedgygdetailed']
    )
}}

SELECT 
		rbukrs as company,
		acdoca.belnr as  document_number,
		acdoca.buzei as line_item,
		racct as account_number,
		year_int = YEAR(CAST(budat AS DATE)),
		month_int = MONTH(CAST(budat AS DATE)),
		fistl as  financial_center_code_old,
		CASE
			WHEN rbukrs = 'RET' THEN (
				CASE 
					WHEN acdoca.rcntr <> '' THEN masraf_yeri_mali_merkez_map.target1
					WHEN acdoca.aufnr <> '' and acdoca.rcntr = '' THEN siparis_mali_merkez_map.target1
				END )
			ELSE masraf_yeri_mali_merkez_map.target1
		END AS financial_center_code_adjusted,
		commitment_item_code_old = fipex,
		commitment_item_code_adjusted = skb1.fipos,
		rcntr as cost_center,
		HSL AS [amount_try],
		KSL AS [amount_eur],
		OSL AS [amount_usd],
		'ACDOCA' as flag
FROM {{ ref('stg__s4hana_t_sap_acdoca_full') }} acdoca
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cmp ON acdoca.rbukrs = cmp.RobiKisaKod
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmfmoarep1000012') }} masraf_yeri_mali_merkez_map ON acdoca.rcntr BETWEEN masraf_yeri_mali_merkez_map.sour1_from AND masraf_yeri_mali_merkez_map.sour1_to
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmfmoarep1000056') }} siparis_mali_merkez_map 
			ON (siparis_mali_merkez_map.sour1_from LIKE '%RET%' AND acdoca.aufnr LIKE '%RET%' AND LEFT(siparis_mali_merkez_map.sour1_from, 3) = LEFT(acdoca.aufnr, 3))
			OR (siparis_mali_merkez_map.sour1_from NOT LIKE '%RET%' AND acdoca.aufnr NOT LIKE '%RET%' AND siparis_mali_merkez_map.sour1_from = acdoca.aufnr)
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_skb1') }} skb1  
			on acdoca.rbukrs = skb1.bukrs
			and acdoca.racct = skb1.saknr

WHERE 1=1
	/* TÜM GRUPLAR */
	AND (LEFT(masraf_yeri_mali_merkez_map.target1, 3) IN (SELECT bukrs from "aws_stage"."s4_odata"."raw__s4hana_t_sap_t001" WHERE f_obsolete = 0) OR
	(LEFT(siparis_mali_merkez_map.target1, 3) IN (SELECT bukrs from "aws_stage"."s4_odata"."raw__s4hana_t_sap_t001" WHERE f_obsolete = 0)))
	AND LEFT(racct,3) IN ('770','740','750')
	AND	NOT (rbukrs = 'HOL'
				AND 
					(  (AUFNR LIKE 'HOL-%') 
					OR (AUFNR LIKE '258%')
					OR (AUFNR LIKE '263%' AND acdoca.BLART = 'AA')
						)
				) 
	/** HOLDING FILTRESI **/
	AND LEFT(skb1.fipos,3) = '100'
	AND (CASE
			WHEN rbukrs = 'RET' THEN (
				CASE 
					WHEN acdoca.rcntr <> '' THEN masraf_yeri_mali_merkez_map.target1
					WHEN acdoca.aufnr <> '' and acdoca.rcntr = '' THEN siparis_mali_merkez_map.target1
				END )
			ELSE masraf_yeri_mali_merkez_map.target1
		END) NOT IN (
        	'HOLDG0005',
        	'HOLBZ0004',
        	'HOLBZ0020',
        	'HOLBZ0021',
        	'HOLBN0009',
        	'HOLBN0010',
        	'HOLBN0011',
        	'HOLBN0012',
        	'HOLBN0008',
        	'HOLYN0001',
        	'HOLDP0032',
        	'HOLBZ0023',
        	'HOLDP0023',
        	'HOLDP0036',
        	'HOLDP0035',
        	'HOLDP0034'
		)
	AND NOT (YEAR(CAST(budat AS DATE)) <> 2025
		AND (CASE
				WHEN rbukrs = 'RET' THEN (
					CASE 
						WHEN acdoca.rcntr <> '' THEN masraf_yeri_mali_merkez_map.target1
						WHEN acdoca.aufnr <> '' and acdoca.rcntr = '' THEN siparis_mali_merkez_map.target1
					END )
				ELSE masraf_yeri_mali_merkez_map.target1
			END) = 'HOLDP0012'
		)
	AND NOT (rcntr IN ('HOLH1703', 'HOLH2001', 'HOLH2002', 'HOLH2003', 'HOLH2051', 'HOLH2013') AND skb1.fipos IN ('100100130', '100100100', '100100110')
		)
	AND NOT ((CASE
				WHEN rbukrs = 'RET' THEN (
					CASE 
						WHEN acdoca.rcntr <> '' THEN masraf_yeri_mali_merkez_map.target1
						WHEN acdoca.aufnr <> '' and acdoca.rcntr = '' THEN siparis_mali_merkez_map.target1
					END )
				ELSE masraf_yeri_mali_merkez_map.target1
			 END) = 'HOLOR0001'
		AND skb1.fipos = '100210110'
		)
	AND rcntr NOT IN ('HOLH1601', 'HOLH1803', 'HOLH0110')
	/** RGY FILTRESI **/
	AND NOT (cmp.KyribaGrup = 'RGYGROUP' AND acdoca.aufnr <> '')

	/** RETGROUP FILTRESI **/
	AND	NOT (
		cmp.KyribaGrup = 'RETGROUP' 
		AND (LEFT(acdoca.racct,3) = '740' OR LEFT(acdoca.racct,3) = '750')
		AND skb1.fipos = '100130100'
		)
		
	/** RET FILTRESI **/
	AND	NOT (
		rbukrs = 'RET' 
		AND racct = '7701101000'
		AND (gkont = '7701101000' or gkont = '7401101000' or gkont = '3930902001')
		AND acdoca.blart = 'SZ'
		)

	AND	NOT (
		rbukrs IN ('BTA', 'RTU', 'RLT', 'ARN')
		AND racct = '7701101000'
		AND gkont = '3810201001'
		AND acdoca.blart = 'SZ'
		)

	AND	NOT (
		rbukrs IN ('RSM', 'RMY', 'TBO') 
		AND LEFT(racct,3) IN ('740','750')
		)

	/** REC FILTRESI **/
		AND rcntr NOT IN (
			'RECY0000',
			'RECY0002',
			'RECY0401',
			'RECY2153',
			'RECY0003',
			'RECY2151',
			'RECY0010',
			'RECY0011',
			'RECY7066',
			'RECY7065',
			'RECY7056',
			'RECY7055',
			'RECY7057',
			'RECY7052',
			'RECY7051',
			'RECY7050',
			'RECY7058',
			'RKZY0103',
			'RMIY0001',
			'RMIY0010',
			'RMIY8006',
			'RMIY0200',
			'RMIY8005',
			'RMIA0084',
			'RECY0301',
			'RMIY0015',
			'RMIY0016',
			'RMIY0017',
			'RMIY0014',
			'RMIY0012',
			'RMIY0018',
			'RMIY8007',
			'RECY7069',
			'RECX0020'
		)

	/** ENERJI FILTRESI **/
	AND NOT (
		rbukrs IN (
			'REN',
			'RES',
			'AVA',
			'TUA',
			'HOS',
			'FIL',
			'AKO',
			'ALC',
			'ASY',
			'SEG',
			'MAM',
			'REH',
			'CKR',
			'TAS',
			'SYI',
			'DCH',
			'EST',
			'EGY',
			'DMR'
			)
		AND (LEFT(acdoca.racct,3) = '740')
		)

	AND rbukrs <> 'BNS'
	AND NOT (rbukrs = N'CTA' AND acdoca.belnr = N'1900000048')