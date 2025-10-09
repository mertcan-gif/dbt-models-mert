{{
  config(
    materialized = 'table',tags = ['gyg_kpi']
    )
}}
WITH gyg_budget_year_month AS (

    SELECT DISTINCT
        FISCYEAR AS fiscal_year,
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
        ,budget_version = REPLACE(fmbh.[version],'Q','V')
    FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmbl') }} fmbl
        LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmfctrt') }} fmfctrt ON fmfctrt.FICTR = fmbl.FUNDSCTR
        LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }}  fmcit ON fmbl.CMMTITEM = fmcit.FIPEX
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
)
SELECT 
    fiscal_year
    ,financial_center_description
    ,financial_center_code
    ,commitment_item_code
    ,commitment_item_definition
    ,year_month
    ,month
    ,budget = case when commitment_item_code = '100190200' and budget_version <> 'V1' then budget*0.2 else budget end -- kıdem ihbarın %20si alınmıştır
    ,budget_version
FROM gyg_budget_year_month
WHERE 1=1
	AND LEFT(commitment_item_code,3)='100'
	AND financial_center_code NOT IN (
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
	AND NOT (fiscal_year <> '2025' 
		AND financial_center_code = 'HOLDP0012')
    
    AND financial_center_code NOT LIKE N'BNS%'


