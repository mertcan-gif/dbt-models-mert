{{
  config(
    materialized = 'table',tags = ['gyg_kpi']
    )
}}
WITH financial_center_commitment_code_mapping AS (
 SELECT DISTINCT
        year_int,
        month_int,
        financial_center,
        commitment_item_code
    FROM (
        SELECT DISTINCT
            FUNDSCTR as financial_center,
            CMMTITEM as commitment_item_code
        FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmbl') }} fmbl
        WHERE LEFT(FUNDSCTR,3) IN (SELECT bukrs from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001') }} where ktopl = 'RONS')

        UNION

        SELECT DISTINCT
            financial_center_code_adjusted,
            commitment_item_code_adjusted
        FROM {{ ref('stg__gyg_kpi_t_fact_realizedgygdetailed') }} acdoca
        WHERE 1=1
        ) commitments
		LEFT JOIN (
			SELECT DISTINCT year_int, month_int
			FROM {{ source('stg_dimensions','raw__dwh_t_dim_dates') }}
			WHERE 1=1
		) date_dim ON 1=1
		  AND year_int IN (2024, 2025)
)
SELECT *
FROM financial_center_commitment_code_mapping
WHERE financial_center NOT LIKE N'BNS%'