{{
  config(
    materialized = 'table',tags = ['gyg_kpi']
    )
}}


/*****  BAŞLANGIÇ: COMPANY ANA JOIN EDILECEK VERI *****/
	-- COMPANYLER İÇİN KARTEZYEN ÇARPANI YAPIYORUM, BÜTÜN VERİLERİ BU KARTEZYEN ÇARPANI İLE OLUŞTURUP BU VERİYE BAĞLAYACAĞIM
WITH		all_companies as (
			select 
				cross_applied.*,
				cmp.FCKisaKod as fc_company
				,[year] = LEFT(year_month,4)
				,[month] = RIGHT(year_month,LEN(year_month)-5)
			from (
				select	distinct year_month,budget_version,erp_company
				from {{ ref('stg__gyg_kpi_t_fact_realizedrevenue') }} --"aws_stage"."gyg_kpi"."stg__gyg_kpi_t_fact_realizedrevenue" 
				CROSS APPLY (
					SELECT DISTINCT budget_version
					FROM  {{ ref('stg__gyg_kpi_t_fact_budgetgyg') }} --"aws_stage"."gyg_kpi"."stg__gyg_kpi_t_fact_budgetgyg"
					) _vers
				CROSS APPLY (
						select distinct RobiKisaKod as erp_company
						FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }}
					) _comp
				) cross_applied
				left join {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cmp ON cross_applied.erp_company = cmp.RobiKisaKod
		)
/*****  BİTİŞ: COMPANY ANA JOIN EDILECEK VERI *****/

/*****  BAŞLANGIÇ: REALIZED REVENUE*****/
	-- FC'de veriler konsolide şekilde que bazlı üstüne eklenerek gittiği için bir o Q'nun gelirini bulmak adına bir önceki Q'dan çıkarıyorum.
		,realized_cte1 as (
				select 
					*
					,left(year_month,4) as [year]
					,CAST(RIGHT(year_month,LEN(year_month)-5) AS INT) as [month]
					,ROW_NUMBER() OVER(PARTITION BY fc_company, LEFT(year_month,4) ORDER BY fc_company, left(year_month,4),cast(RIGHT(year_month,LEN(year_month)-5) as INT)) year_rank
				from {{ ref('stg__gyg_kpi_t_fact_realizedrevenue') }} fc_revenue
				)
		,realized_final as (
				SELECT 
					cte1.*
					,coalesce(cte1.revenue_try,0)-coalesce(cte2.revenue_try,0) realized_revenue_try
					,coalesce(cte1.revenue_eur,0)-coalesce(cte2.revenue_eur,0) realized_revenue_eur
				FROM realized_cte1 cte1
					left join realized_cte1 cte2
						ON cte1.fc_company = cte2.fc_company
						and cte1.[year] = cte2.[year]
						and cte1.year_rank-1 = cte2.year_rank
				)
/*****  BİTİŞ: REALIZED REVENUE*****/



/*****  BAŞLANGIÇ: FC GYG *****/
	--FC'de veriler konsolide şekilde que bazlı üstüne eklenerek gittiği için bir o Q'nun gelirini bulmak adına bir önceki Q'dan çıkarıyorum.
			, gyg_realized_fc_cte_raw AS (
					SELECT 
						year_month
						,year = CAST(SUBSTRING(year_month,0,5) AS INT)
						,month = CAST(SUBSTRING(year_month,6,5) AS INT)
						,KyribaGrup AS [group]
						,FCKisaKod AS fc_company
						,cmp.RobiKisaKod AS company
						,sum(consamount/eur_currency)*-1 as eur_value
						,sum(consamount)*-1 as try_value
					FROM {{ source('stg_fc_kpi', 'raw__fc_kpi_t_fact_fcalldetails') }} fc 
						LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cmp ON cmp.FCKisaKod = fc.entity
					WHERE 1=1 
					and stot1 in ('General administrative expenses' , 'Marketing, Selling and Distribution expenses')
					GROUP BY 
						year_month
						,SUBSTRING(year_month,0,5)
						,SUBSTRING(year_month,6,5)
						,KyribaGrup
						,FCKisaKod
						,cmp.RobiKisaKod
				)
			, gyg_realized_cte_1 as (
					select 
						*
						,ROW_NUMBER() OVER(PARTITION BY fc_company, [year] ORDER BY fc_company, [year],[month]) year_rank
					from gyg_realized_fc_cte_raw
			)
			, gyg_realized_fc AS (
				SELECT 
					cte1.*
					,coalesce(cte1.try_value,0)-coalesce(cte2.try_value,0) realized_fc_gyg_try
					,coalesce(cte1.eur_value,0)-coalesce(cte2.eur_value,0) realized_fc_gyg_eur
				FROM gyg_realized_cte_1 cte1
					left join gyg_realized_cte_1 cte2
						ON cte1.fc_company = cte2.fc_company
						and cte1.[year] = cte2.[year]
						and cte1.year_rank-1 = cte2.year_rank
			)
/*****  BAŞLANGIÇ: FC GYG *****/


/*****  BAŞLANGIÇ: S4HANA GYG BÜTÇESİ *****/
		,gyg_budget AS (
			SELECT 
				fiscal_year
				,quarter_month
				,year_month = concat(fiscal_year,'-',quarter_month) 
				,LEFT(financial_center_code,3) company
				,SUM(budget)*-1 as budget_eur
				,budget_version
			FROM
				(
				SELECT
					   [fiscal_year]
					  ,[financial_center_description]
					  ,[financial_center_code]
					  ,[commitment_item_code]
					  ,[commitment_item_definition]
					  ,[year_month]
					  ,[month]
					  ,quarter_month =
						 CASE	
							WHEN [Month] <=3 THEN 3
							WHEN [Month] <=6 THEN 6
							WHEN [Month] <=9 THEN 9
							ELSE 12
						END
					  ,[budget]
					  ,[budget_version]
				FROM {{ ref('stg__gyg_kpi_t_fact_budgetgyg') }}
			) bgt
			GROUP BY
				fiscal_year
				,quarter_month
				,LEFT(financial_center_code,3)
				,budget_version
		)
/*****  BİTİŞ: S4HANA GYG BÜTÇESİ *****/


,final AS (
	SELECT 
		dim_comp.rls_region
		,dim_comp.rls_group
		,dim_comp.rls_company
		,dim_comp.rls_businessarea
		,_all.[year] AS fiscal_year
		,_all.[month] AS quarter_month
		,dim_comp.KyribaGrup AS [group]
		,_all.erp_company AS [company]
		,gyg_budget.budget_eur
		,realized_fc_gyg_eur 
		,realized_fc_gyg_try 
		,realized_final.realized_revenue_eur
		,realized_final.realized_revenue_try 
		,_all.budget_version
	FROM all_companies _all
		LEFT JOIN gyg_budget on 
				 gyg_budget.company = _all.erp_company
			and  gyg_budget.budget_version =_all.budget_version
			and gyg_budget.year_month = _all.year_month
		LEFT JOIN gyg_realized_fc
					ON _all.year_month = gyg_realized_fc.year_month
					AND _all.erp_company = gyg_realized_fc.company
		LEFT JOIN realized_final 
				ON _all.fc_company = realized_final.fc_company
				AND _all.year_month = realized_final.year_month
		LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp
				ON dim_comp.RobiKisaKod = _all.[erp_company]
		WHERE dim_comp.kyriba_ust_group = N'RÖNESANS'

)
SELECT
	*
FROM final
WHERE 1=1
	and [group] NOT IN ('DESNAGROUP' , 'CEYGROUP' , 'KZAGROUP' , 'CLOSED')