{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

WITH rgy_operating_revenue_budget AS (

    SELECT DISTINCT
        FISCYEAR AS fiscal_year,
		CASE
			WHEN fmbl.FUNDSCTR = N'ALTAVM001' THEN '17'
			WHEN fmbl.FUNDSCTR = N'BAKAVM001' THEN '23'
			WHEN fmbl.FUNDSCTR = N'ESEAVM001' THEN '8'
			WHEN fmbl.FUNDSCTR = N'FERAVM001' THEN '2'
			WHEN fmbl.FUNDSCTR = N'GOKAVM001' THEN '5'
			WHEN fmbl.FUNDSCTR = N'KOZAVM001' THEN '4'
			WHEN fmbl.FUNDSCTR = N'KURAVM001' THEN '1'
			WHEN fmbl.FUNDSCTR = N'MELAVM001' THEN '9'
			WHEN fmbl.FUNDSCTR = N'ML3AVM001' THEN '10'
			WHEN fmbl.FUNDSCTR = N'ML4AVM001' THEN '11'
			WHEN fmbl.FUNDSCTR = N'SALAVM001' THEN '15'
			WHEN fmbl.FUNDSCTR = N'TARAVM001' THEN '14'
		END AS portfolio_id,
        fmfctrt.MCTXT AS financial_center_description,
        fmbl.FUNDSCTR AS financial_center_code,
        CMMTITEM AS commitment_item_code,
        fmcit.TEXT1 AS commitment_item_definition,
        CONCAT(FISCYEAR, '-', RIGHT('0' + CAST(MonthValues.month AS VARCHAR(2)), 2)) AS year_month,
        MonthValues.[month],
        SUM(        
            CASE 
                WHEN RIGHT(MonthValues.Value,1) = '-'
                THEN -1*cast(replace(MonthValues.Value,'-','') as money)
                else cast (MonthValues.Value as money)
            END
            ) AS budget
        --,budget_version = REPLACE(fmbh.[version],'Q','V')
    FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmbl') }} fmbl
        LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmfctrt') }} fmfctrt ON fmfctrt.FICTR = fmbl.FUNDSCTR
        LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} fmcit ON fmbl.CMMTITEM = fmcit.FIPEX
        RIGHT JOIN ( -- Bütçe Versiyonları Buradan Gelmektedir
            SELECT distinct docnr,docyear,[version] 
            FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmbh') }}
            WHERE 1=1
                AND [version] <> '0'
		        AND [version] <> 'R0'
        ) fmbh ON fmbl.docnr= fmbh.docnr and fmbh.docyear = fmbl.docyear
        CROSS APPLY (
            VALUES
                (1, fmbl.TVAL01),
                (2, fmbl.TVAL02),
                (3, fmbl.TVAL03),
                (4, fmbl.TVAL04),
                (5, fmbl.TVAL05),
                (6, fmbl.TVAL06),
                (7, fmbl.TVAL07),
                (8, fmbl.TVAL08),
                (9, fmbl.TVAL09),
                (10, fmbl.TVAL10),
                (11, fmbl.TVAL11),
                (12, fmbl.TVAL12)
        ) AS MonthValues (month, Value)
    WHERE 1 = 1
        AND LEFT(FUNDSCTR,3) IN (SELECT bukrs from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001') }} where ktopl = 'RONS')
    GROUP BY
        fmbl.FUNDSCTR,
        fmfctrt.MCTXT,
        fmbl.CMMTITEM,
        fmcit.TEXT1,
        FISCYEAR,
        CONCAT(FISCYEAR, '-', RIGHT('0' + CAST(MonthValues.month AS VARCHAR(2)), 2)),
        MonthValues.[month],
        fmbh.[version]
),

currency AS (
	SELECT 
		YEAR(CONVERT(DATE, gdatu, 104)) AS [year],
		MONTH(CONVERT(DATE, gdatu, 104)) AS [month],
		CONVERT(DATE, gdatu, 104) AS full_date,
		FCURR,
		CAST(UKURS AS MONEY) AS rate
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurr') }} AS T
	WHERE 1=1
		AND (TCURR = 'TRY') 
		AND (KURST = 'BTG') 
),

min_date AS (
	SELECT
		[year],
		[month],
		MIN(full_date) AS first_day
	FROM currency
	GROUP BY [year], [month]
),

currencies AS(
	SELECT 
		md.[year], 
		md.[month], 
		md.first_day,
		MAX(CASE WHEN c.FCURR = 'EUR' THEN c.rate END) AS try_value,
		MAX(CASE WHEN c.FCURR = 'USD' THEN c.rate END) AS usd_value
	FROM min_date md
	JOIN currency c ON md.[year] = c.[year] 
					AND md.[month] = c.[month] 
					AND md.first_day = c.full_date
	GROUP BY md.[year], md.[month], md.first_day
)

SELECT
    portfolio_id
	,p.[Name] AS portfolio_name
    ,fiscal_year
    ,financial_center_description
    ,financial_center_code
    ,commitment_item_code
    ,commitment_item_definition
    ,year_month
    ,budget.[month]
	,budget = budget * try_value
	,budget_eur = budget
	,budget_usd = budget * usd_value
    ,scope = 'Operating Revenue'
FROM rgy_operating_revenue_budget budget
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} p ON budget.portfolio_id = p.ID
LEFT JOIN currencies ON budget.fiscal_year = currencies.[year]
					 AND budget.[month] = currencies.[month]
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
    portfolio_id
	,p.[Name] AS portfolio_name
    ,fiscal_year
    ,financial_center_description
    ,financial_center_code
    ,commitment_item_code
    ,commitment_item_definition
    ,year_month
    ,budget.[month]
	,budget = budget * try_value * (-1)
	,budget_eur = budget * (-1)
	,budget_usd = budget * usd_value * (-1)
    ,scope = 'Maintenance & Repair'
FROM rgy_operating_revenue_budget budget
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} p ON budget.portfolio_id = p.ID
LEFT JOIN currencies ON budget.fiscal_year = currencies.[year]
					 AND budget.[month] = currencies.[month]
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
