
{{
  config(
    materialized = 'table',tags = ['rmore','fi_kpi']
    )
}}

/* 
Date: 20240521
Creator: Adem Numan Kaya
Report Owner: Adem Numan Kaya
Explanation: Bu tablo, bütçe ve gerçekleşen verilerini bir araya getirerek bütçe gerçekleşme oranlarını hesaplamak için kullanılır. Bütçe verileri `stg__fi_kpi_t_fact_budget` tablosundan, gerçekleşen veriler ise SAP ACDOCA tablosundan (`stg__s4hana_t_sap_acdoca_full`) alınmaktadır. `stg__fi_kpi_t_dim_budgetmappingwithhierarcy` tablosu ana iskeleti oluşturur ve bu iki veri setini mali merkez (financial center) ve taahhüt kalemi (commitment item) bazında birleştirir.
*/



with fmi as (
SELECT
	YEAR(CAST(budat AS DATE)) AS year_int
	,MONTH(CAST(budat AS DATE)) as month_int
	,fistl as financial_center_code
	,fipex as commitment_item_code
	,amount_try = SUM(CAST(HSL AS MONEY))
	,amount_usd = SUM(CAST(OSL AS MONEY))
	,amount_eur = SUM(CAST(KSL AS MONEY))
from {{ ref('stg__s4hana_t_sap_acdoca_full') }}
GROUP BY
	YEAR(CAST(budat AS DATE))
	,MONTH(CAST(budat AS DATE))
	,fistl 
	,fipex
)

,final_data AS (
	SELECT
		fcc.year_int AS fiscal_year
		,fcc.financial_center_code
		,fcc.commitment_item_code 
		,fmcit.TEXT1 as commitment_item_code_description
		,fcc.month_int as [month]
		,CONCAT(fcc.year_int, '-', RIGHT('0' + CAST(fcc.month_int AS VARCHAR(2)), 2)) AS year_month
		,CAST(COALESCE(budget.budget*(-1), 0) AS MONEY) AS 'budget_eur'
		,CAST(COALESCE(budget.budget*(-1)*curr.eur_to_try/curr.usd_to_try, 0) AS MONEY) AS 'budget_usd'
		,CAST(COALESCE(budget.budget*(-1)*curr.eur_to_try, 0) AS MONEY) AS 'budget_try'
		,CAST(COALESCE(fmi.amount_eur, 0) AS MONEY) AS realized_eur
		,CAST(COALESCE(fmi.amount_usd, 0) AS MONEY) AS realized_usd
		,CAST(COALESCE(fmi.amount_try, 0) AS MONEY) AS realized_try
		,fcc.budget_version
		,level_4=fcc.fipex1
		,level_3=fcc.fipex2
		,level_2=fcc.fipex3
		,level_1=fcc.fipex4
		,level_4_text=fmcit_1.TEXT1
		,level_3_text=fmcit_2.TEXT1
		,level_2_text=fmcit_3.TEXT1
		,level_1_text=fmcit_4.TEXT1
		,is_adjustment = 0
	FROM {{ ref('stg__fi_kpi_t_dim_budgetmappingwithhierarcy') }} fcc
	LEFT JOIN {{ ref('stg__fi_kpi_t_fact_budget') }} budget ON fcc.financial_center_code = budget.financial_center_code
		AND fcc.commitment_item_code = budget.commitment_item_code
		AND fcc.year_int = budget.fiscal_year
		AND fcc.month_int = budget.[Month]
		and fcc.budget_version = budget.budget_version 
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} AS fmcit ON fmcit.FIPEX = fcc.commitment_item_code 
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} AS fmcit_1 ON fmcit_1.FIPEX = fcc.fipex1
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} AS fmcit_2 ON fmcit_2.FIPEX = fcc.fipex2
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} AS fmcit_3 ON fmcit_3.FIPEX = fcc.fipex3
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} AS fmcit_4 ON fmcit_4.FIPEX = fcc.fipex4
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmfctrt') }} AS fmfctrt ON fcc.financial_center_code = fmfctrt.FICTR
	LEFT JOIN {{ ref('stg__gyg_kpi_t_dim_currencies') }} AS curr ON fcc.year_int = curr.[year] and fcc.month_int = curr.[month]
	LEFT JOIN fmi as fmi ON  
		fcc.financial_center_code = fmi.financial_center_code COLLATE database_default 
		AND fcc.commitment_item_code = fmi.commitment_item_code COLLATE database_default
		AND fcc.year_int = fmi.year_int
		AND fcc.month_int = fmi.month_int
	--WHERE dim_comp.kyriba_ust_group = N'RÖNESANS' OR dim_comp.kyriba_ust_group = N'NONGR'
)

SELECT
	dim_comp.rls_region
	,dim_comp.rls_group
	,dim_comp.rls_company
	,dim_comp.rls_businessarea
	,dim_comp.KyribaGrup as [group]
	,dim_comp.RobiKisaKod as company
	,final_data.*
FROM final_data
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON  dim_comp.RobiKisaKod = LEFT(final_data.financial_center_code,3)
WHERE 1=1