{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

WITH realized_operating_revenue AS (
SELECT 
		CASE
			WHEN fistl = N'ALTAVM001' THEN '17'
			WHEN fistl = N'BAKAVM001' THEN '23'
			WHEN fistl = N'ESEAVM001' THEN '8'
			WHEN fistl = N'FERAVM001' THEN '2'
			WHEN fistl = N'GOKAVM001' THEN '5'
			WHEN fistl = N'KOZAVM001' THEN '4'
			WHEN fistl = N'KURAVM001' THEN '1'
			WHEN fistl = N'MELAVM001' THEN '9'
			WHEN fistl = N'ML3AVM001' THEN '10'
			WHEN fistl = N'ML4AVM001' THEN '11'
			WHEN fistl = N'SALAVM001' THEN '15'
			WHEN fistl = N'TARAVM001' THEN '14'
		END AS portfolio_id,
		bseg.bukrs AS company,
		bseg.belnr AS  document_number,
		hkont AS account_number,
		year_int = YEAR(CAST(hbudat AS DATE)),
		month_int = MONTH(CAST(hbudat AS DATE)),
		fistl AS  financial_center_code,
		bseg.fipos AS commitment_item_code,
		fm.MCTXT AS commitment_item_description,
		[amount_try] = (CASE WHEN shkzg = 'H' THEN CAST(dmbtr AS float) ELSE 0 END) - (CASE WHEN shkzg = 'S' THEN CAST(dmbtr AS float) ELSE 0 END),
		[amount_eur] = (CASE WHEN shkzg = 'H' THEN CAST(dmbe2 AS float) ELSE 0 END) - (CASE WHEN shkzg = 'S' THEN CAST(dmbe2 AS float) ELSE 0 END),
		[amount_usd] = (CASE WHEN shkzg = 'H' THEN CAST(dmbe3 AS float) ELSE 0 END) - (CASE WHEN shkzg = 'S' THEN CAST(dmbe3 AS float) ELSE 0 END)
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_bseg') }} bseg
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} fm ON bseg.fipos = fm.fipex
)

SELECT 
p.[Name] AS portfolio_name,
actual.*,
scope = 'Operating Revenue'
FROM realized_operating_revenue actual
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} p ON actual.portfolio_id = p.ID
WHERE 1=1
	AND commitment_item_code IN (
		'400600102',
		'400600103',
		'400600201',
		'400600301',
		'400600303'
	)

	AND financial_center_code IN (
		'ALTAVM001',
		'BAKAVM001',
		'ESEAVM001',
		'FERAVM001',
		'GOKAVM001',
		'KOZAVM001',
		'KURAVM001',
		'MELAVM001',
		'ML3AVM001',
		'ML4AVM001',
		'SALAVM001',
		'TARAVM001'
		)

UNION ALL

SELECT 
p.[Name] AS portfolio_name,
actual.portfolio_id,
actual.[company],
actual.document_number,
actual.account_number,
actual.year_int,
actual.month_int,
actual.financial_center_code,
actual.commitment_item_code,
actual.commitment_item_description,
actual.amount_try * (-1),
actual.amount_eur * (-1),
actual.amount_usd * (-1),
scope = 'Maintenance & Repair'
FROM realized_operating_revenue actual
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} p ON actual.portfolio_id = p.ID
WHERE 1=1
	AND commitment_item_code IN (
        '400020102',
        '400020103',
        '400020104',
        '400020105',
        '400020106',
        '400020107',
        '400020108',
        '400020109',
        '400020110',
        '400020111',
        '400020112',
        '400020203',
        '400020204',
        '400020205',
        '400020206',
        '400020207',
        '400020208',
        '400020209',
        '400020301',
        '400070101',
        '400070102',
        '400070103'
	)

	AND financial_center_code IN (
		'ALTAVM001',
		'BAKAVM001',
		'ESEAVM001',
		'FERAVM001',
		'GOKAVM001',
		'KOZAVM001',
		'KURAVM001',
		'MELAVM001',
		'ML3AVM001',
		'ML4AVM001',
		'SALAVM001',
		'TARAVM001'
		)
