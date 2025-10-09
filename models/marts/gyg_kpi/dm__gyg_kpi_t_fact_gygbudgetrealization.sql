
{{
  config(
    materialized = 'table',tags = ['gyg_kpi']
    )
}}

WITH final_data AS (

	SELECT
		dim_comp.rls_region
		,dim_comp.rls_group
		,dim_comp.rls_company
		,dim_comp.rls_businessarea
		,dim_comp.KyribaGrup as [group]
		,dim_comp.RobiKisaKod as company
		,hy.zfi_cati AS parent_company
		,fcc.year_int AS fiscal_year
		,fcc.financial_center AS financial_center_code
		,fmfctrt.MCTXT as financial_center_description
		,fcc.commitment_item_code
		,fmcit.TEXT1 as commitment_item_code_description
		,fmci.fipup AS high_level_commitment_item_code
		,fmci.text1 AS high_level_commitment_item_description
		,fcc.month_int as [month]
		,CONCAT(fcc.year_int, '-', RIGHT('0' + CAST(fcc.month_int AS VARCHAR(2)), 2)) AS year_month
		,COALESCE(budget.budget*(-1), 0) AS 'budget_eur'
		,COALESCE(budget.budget*(-1)*curr.eur_to_try/curr.usd_to_try, 0) AS 'budget_usd'
		,COALESCE(budget.budget*(-1)*curr.eur_to_try, 0) AS 'budget_try'
		,COALESCE(fmi.amount_eur, 0) AS realized_eur
		,COALESCE(fmi.amount_usd, 0) AS realized_usd
		,COALESCE(fmi.amount_try, 0) AS realized_try
		,fcc.budget_version
		,is_adjustment = 0
	FROM (
		SELECT *
			FROM {{ ref('stg__gyg_kpi_t_dim_gygmapping') }} fcc
			CROSS APPLY 
				(SELECT DISTINCT budget_version
						from  {{ ref('stg__gyg_kpi_t_fact_budgetgyg') }}) v

		) fcc
		LEFT JOIN {{ ref('stg__gyg_kpi_t_fact_budgetgyg') }} budget ON fcc.financial_center = budget.financial_center_code
			AND fcc.commitment_item_code = budget.commitment_item_code
			AND fcc.year_int = budget.fiscal_year
			AND fcc.month_int = budget.[Month]
			and fcc.budget_version = budget.budget_version 
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmfctrt') }} AS fmfctrt ON fcc.financial_center = fmfctrt.FICTR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} AS fmcit ON fcc.commitment_item_code = fmcit.FIPEX COLLATE database_default
		LEFT JOIN (
			SELECT f1.fipex, fipup, text1
				FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmci') }} f1
				LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} f2 ON f1.fipup = f2.fipex) AS fmci ON fcc.commitment_item_code = fmci.fipex
		LEFT JOIN {{ ref('stg__gyg_kpi_t_dim_currencies') }} AS curr ON fcc.year_int = curr.[year] and fcc.month_int = curr.[month]
		LEFT JOIN {{ ref('stg__gyg_kpi_t_fact_realizedgyg') }} fmi ON 
			fcc.financial_center = fmi.financial_center_code COLLATE database_default 
			AND fcc.commitment_item_code = fmi.commitment_item_code COLLATE database_default
			AND fcc.year_int = fmi.year_int
			AND fcc.month_int = fmi.month_int
		LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp 
			ON  dim_comp.RobiKisaKod = LEFT(fcc.financial_center,3)
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfi_sk_hiyerarsi') }} hy ON dim_comp.RobiKisaKod = hy.bukrs
		WHERE dim_comp.kyriba_ust_group = N'RÖNESANS' OR dim_comp.kyriba_ust_group = N'NONGR'
)
SELECT *
FROM final_data
WHERE 1=1

UNION ALL

SELECT
	  [rls_region]
	  ,[rls_group]
	  ,[rls_company]
	  ,[rls_businessarea]
	  ,[KyribaGrup] as [group]
	  ,[company]
	  ,hy.zfi_cati AS parent_company
      ,[fiscal_year]
      ,[financial_center_code]
      ,[financial_center_description]
      ,[commitment_item_code]
      ,[commitment_item_code_description]
	  ,COALESCE(fmc.fipup, ga.high_level_commitment_item_code) AS high_level_commitment_item_code
	  ,COALESCE(fmc.text1, ga.high_level_commitment_item_description) AS high_level_commitment_item_description
      ,[month]
      ,[year_month]
      ,[budget_eur]
      ,[budget_usd]
      ,[budget_try]
      ,[realized_eur]
      ,[realized_usd]
      ,[realized_try]
      ,[budget_version]
	  ,is_adjustment = 1
  FROM {{ source('stg_sharepoint', 'raw__gyg_kpi_t_fact_gaadjustments') }} ga
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} AS fmcit ON ga.commitment_item_code = fmcit.FIPEX COLLATE database_default
  LEFT JOIN (
	SELECT f1.fipex, fipup, text1
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmci') }} f1
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} f2 ON f1.fipup = f2.fipex) AS fmc ON ga.commitment_item_code = fmc.fipex
  LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON dim_comp.RobiKisaKod = ga.company
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfi_sk_hiyerarsi') }} hy ON dim_comp.RobiKisaKod = hy.bukrs
