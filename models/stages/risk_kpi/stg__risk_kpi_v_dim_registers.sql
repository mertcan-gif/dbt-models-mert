{{
  config(
    materialized = 'table',tags = ['risk_kpi']
    )
}}
WITH main_data as (
SELECT
	r.*,
	op.Profit as opening_profit,
	op.CalcRealCasePrice as opening_real_case_price,
	LAG(r.Profit) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID) ASC,r.YearQuarterID ASC ) as lagged_profit,
	r.Profit - (LAG(r.Profit) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID),r.YearQuarterID ASC )) as profit_minus_previous_quarter_profit,
	r.Profit - op.Profit as profit_minus_opening_quarter_profit,
	LAG(r.CalcBestCasePrice) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID),r.YearQuarterID ASC ) as lagged_bestcase,
	LAG(r.CalcWorstCasePrice) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID),r.YearQuarterID ASC ) as lagged_worstcase,
	LAG(r.CalcRealCasePrice) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID),r.YearQuarterID ASC ) as lagged_realcase,
	r.Profit - r.CalcRealCasePrice as profit_mins_real_case_price,
	LAG(r.Profit) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID),r.YearQuarterID ASC ) - LAG(r.CalcRealCasePrice) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID),r.YearQuarterID ASC ) as lagged_profit_minus_lagged_real_case_price,
	LAG(cast(r.Revenue as DECIMAL(20,6))) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID),r.YearQuarterID ASC ) as lagged_revenue,
	cast(op.Revenue as DECIMAL(20,6)) as opening_revenue,
	LAG(cast(r.ExpenseBudget as DECIMAL(20,6))) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID),r.YearQuarterID ASC ) as lagged_expense_budget,
	cast(op.ExpenseBudget as DECIMAL(20,6)) as opening_expensebudget,
	LAG(r.Revenue) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID),r.YearQuarterID ASC )/NULLIF(LAG(r.ExpenseBudget) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID),r.YearQuarterID ASC ),0) as lagged_revenue_over_cost,
	op.Revenue/NULLIF(op.ExpenseBudget,0) as opening_revenue_over_cost,
	op.ProfitPercent as opening_profit_percent,
	LAG(r.ProfitPercent) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID),r.YearQuarterID ASC ) as lagged_profitpercent,



case
	when r.YearQuarterID = '1' THEN CONCAT('Tender', r.Year )
	when r.YearQuarterID = '2' THEN CONCAT('Opening',r.Year )
	when r.YearQuarterID = '3' THEN CONCAT( r.Year,'-','Q1')
	when r.YearQuarterID = '4' THEN CONCAT( r.Year,'-','Q2')
	when r.YearQuarterID = '5' THEN CONCAT( r.Year,'-', 'Q3')
	when r.YearQuarterID = '6' THEN CONCAT(r.Year,'-','Q4')
ELSE NULL END AS  year_quarter,
CASE
	WHEN DENSE_RANK() OVER (
    PARTITION BY r.projectID
    ORDER BY r.Year ASC, 
            r.YearQuarterID ASC
)<=2 THEN '1' 
	WHEN DENSE_RANK() OVER (
    PARTITION BY r.projectID
    ORDER BY r.Year DESC, 
            r.YearQuarterID desc
	)<=2 THEN '1' 
	else null
	end as  is_firsttwo_or_lasttwo,
    cast(c.Value as DECIMAL(20,6)) as tl,
    cast(c1.Value as DECIMAL(20,6)) as usd,
    cast(c2.Value as DECIMAL(20,6)) as eur,
    cast(c3.Value as DECIMAL(20,6)) as rub,
	LAG(cast(c.Value as DECIMAL(20,6))) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID) ASC,r.YearQuarterID ASC ) as lagged_tl,
	LAG(cast(c1.Value as DECIMAL(20,6))) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID) ASC,r.YearQuarterID ASC ) as lagged_usd,
	LAG(cast(c2.Value as DECIMAL(20,6))) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID) ASC,r.YearQuarterID ASC ) as lagged_eur,
	LAG(cast(c3.Value as DECIMAL(20,6))) Over(PARTITION by r.ProjectID Order by coalesce(r.Year,r.YearQuarterID) ASC,r.YearQuarterID ASC ) as lagged_rub,
	cast(op.tl as DECIMAL(20,6)) as opening_tl,
	cast(op.usd as DECIMAL(20,6)) as opening_usd,
	cast(op.eur as DECIMAL(20,6)) as opening_eur,
	cast(op.rub as DECIMAL(20,6)) as opening_rub,
	 case  
	WHEN DENSE_RANK() OVER (
    PARTITION BY r.ProjectID
    ORDER BY r.Year DESC, 
             r.YearQuarterID desc
	)<=1 THEN '1' 
	else null
	end as  is_last_one
FROM PRODAPPSDB.RNS_RISK_PROD.[dbo].RGR_REGISTERS r
        left join PRODAPPSDB.RNS_RISK_PROD.[dbo].RGR_REGISTER_CURRENCIES c on c.RegisterID = r.ObjectID and c.DisplayName = 'TL' and c.IsActive  = '1' and c.IsDeleted = '0'
        left join PRODAPPSDB.RNS_RISK_PROD.[dbo].RGR_REGISTER_CURRENCIES c1 on c1.RegisterID = r.ObjectID and c1.DisplayName = 'USD' and c1.IsActive  = '1' and c1.IsDeleted = '0'
        left join PRODAPPSDB.RNS_RISK_PROD.[dbo].RGR_REGISTER_CURRENCIES c2 on c2.RegisterID = r.ObjectID and c2.DisplayName = 'EUR' and c2.IsActive  = '1' and c2.IsDeleted = '0'
        left join PRODAPPSDB.RNS_RISK_PROD.[dbo].RGR_REGISTER_CURRENCIES c3 on c3.RegisterID = r.ObjectID and c3.DisplayName = 'RUB' and c3.IsActive  = '1' and c3.IsDeleted = '0'
LEFT JOIN (select
			m.*,
			c.Value as tl,
			c1.Value as usd,
			c2.Value as eur,
			c3.Value as rub
			from PRODAPPSDB.RNS_RISK_PROD.[dbo].[RGR_REGISTERS] m
			left join PRODAPPSDB.RNS_RISK_PROD.[dbo].RGR_REGISTER_CURRENCIES c on c.RegisterID = m.ObjectID and c.DisplayName = 'TL' and c.IsActive  = '1' and c.IsDeleted = '0'
			left join PRODAPPSDB.RNS_RISK_PROD.[dbo].RGR_REGISTER_CURRENCIES c1 on c1.RegisterID = m.ObjectID and c1.DisplayName = 'USD' and c1.IsActive  = '1' and c1.IsDeleted = '0'
			left join PRODAPPSDB.RNS_RISK_PROD.[dbo].RGR_REGISTER_CURRENCIES c2 on c2.RegisterID = m.ObjectID and c2.DisplayName = 'EUR' and c2.IsActive  = '1' and c2.IsDeleted = '0'
			left join PRODAPPSDB.RNS_RISK_PROD.[dbo].RGR_REGISTER_CURRENCIES c3 on c3.RegisterID = m.ObjectID and c3.DisplayName = 'RUB' and c3.IsActive  = '1' and c3.IsDeleted = '0'
			where 1=1
			 and m.IsActive = '1'
			 and m.IsDeleted = '0'
			 and m.YearQuarterID = '2') op on op.ProjectID = r.ProjectID
WHERE 1=1
	AND r.IsDeleted = '0'
	AND r.IsActive  = '1'
	and r.StatusID = '3'
),
final_cte as (
SELECT


	   [ObjectID]
      ,[Code]
      ,[StatusID]
      ,[ProjectID]
      ,[RiskUnitID]
      ,[TemplateProcessID]
      ,[RiskImpactID]
      ,[RiskGroupID]
      ,[YearQuarterID]
      ,[RequestUserID]
      ,[ContractCompletionDate]
      ,[PlannedCompletionDate]
      ,[ProjectDuration]
      ,[CalcWorstCasePrice]
      ,[CalcBestCasePrice]
      ,[IsActive]
      ,[CreatedDate]
      ,[CompanyID]
      ,[ProfitPercent]
      ,[Year]
      ,[ContractPrice],
	  is_last_one,

(ProfitPercent - lagged_profitpercent) as profitpercent_minus_laggedprofitpercent,
(ProfitPercent - lagged_profitpercent) /NULLIF(lagged_profitpercent,0) as profitpercent_over_laggedprofitpercent,
(ProfitPercent - opening_profit_percent)  as profitpercent_minus_openingprofitpercent,
(ProfitPercent - opening_profit_percent) /NULLIF(opening_profit_percent,0) as profitpercent_over_openingprofitpercent,


is_firsttwo_or_lasttwo,
year_quarter,
Profit as profit,
Profit*tl as profit_tl,
Profit*usd as profit_usd,
Profit*eur as profit_eur,
Profit*rub as profit_rub,

lagged_profit as lagged_profit,
lagged_profit*lagged_tl as lagged_profit_tl ,
lagged_profit*lagged_usd as lagged_profit_usd,
lagged_profit*lagged_eur as lagged_profit_eur,
lagged_profit*lagged_rub as lagged_profit_rub,

opening_profit,
opening_profit*opening_tl as opening_tl,
opening_profit*opening_usd as opening_usd,
opening_profit*opening_eur as opening_eur,
opening_profit*opening_rub as opening_rub,

opening_real_case_price,
opening_real_case_price*opening_tl as opening_real_case_tl,
opening_real_case_price*opening_usd as opening_real_case_usd,
opening_real_case_price*opening_eur as opening_real_case_eur,
opening_real_case_price*opening_rub as opening_real_case_rub,

CalcRealCasePrice,
CalcRealCasePrice*tl as real_case_price_tl,
CalcRealCasePrice*usd as real_case_price_usd,
CalcRealCasePrice*eur as real_case_price_eur,
CalcRealCasePrice*rub as real_case_price_rub,

lagged_realcase,
lagged_realcase*lagged_tl as lagged_real_case_tl,
lagged_realcase*lagged_usd as lagged_real_case_usd,
lagged_realcase*lagged_eur as lagged_real_case_eur,
lagged_realcase*lagged_rub as lagged_real_case_rub,

Revenue,
cast (Revenue as DECIMAL(20,6))*cast(tl as DECIMAL(20,6))  as revenue_tl,
cast (Revenue as DECIMAL(20,6))*cast(usd as DECIMAL(20,6))  as revenue_usd,
cast (Revenue as DECIMAL(20,6))*cast(eur as DECIMAL(20,6))  as revenue_eur,
cast (Revenue as DECIMAL(20,6))*cast(rub as DECIMAL(20,6))  as revenue_rub,

lagged_revenue,
lagged_revenue*lagged_tl as lagged_revenue_tl,
lagged_revenue*lagged_usd as lagged_revenue_usd,
lagged_revenue*lagged_eur as lagged_revenue_eur,
lagged_revenue*lagged_rub as lagged_revenue_rub,

opening_revenue,
opening_revenue*opening_tl as opening_revenue_tl,
opening_revenue*opening_usd as opening_revenue_usd,
opening_revenue*opening_eur as opening_revenue_eur,
opening_revenue*opening_rub as opening_revenue_rub,

ExpenseBudget,
cast(ExpenseBudget as DECIMAL(20,6))*tl as expense_budget_tl,
cast(ExpenseBudget as DECIMAL(20,6))*usd as expense_budget_usd,
cast(ExpenseBudget as DECIMAL(20,6))*eur as expense_budget_eur,
cast(ExpenseBudget as DECIMAL(20,6))*rub as expense_budget_rub,

lagged_expense_budget,
lagged_expense_budget*lagged_tl as lagged_expense_budget_tl,
lagged_expense_budget*lagged_usd as lagged_expense_budget_usd,
lagged_expense_budget*lagged_eur as lagged_expense_budget_eur,
lagged_expense_budget*lagged_rub as lagged_expense_budget_rub,

opening_expensebudget,
opening_expensebudget*opening_tl as opening_expense_budget_tl,
opening_expensebudget*opening_usd as opening_expense_budget_usd,
opening_expensebudget*opening_eur as opening_expense_budget_eur,
opening_expensebudget*opening_rub as opening_expense_budget_rub,
tl,
usd,
eur,
rub

FROM main_data
),
final_cte_2 as (
SELECT
	 [ObjectID] as object_id
	,[Code] as code
	,[StatusID] as status_id
	,[ProjectID] as project_id
	,[RiskUnitID] as risk_unit_id
	,[TemplateProcessID] as template_process_id
	,[RiskImpactID] as risk_impact_id
	,[RiskGroupID] as risk_group_id
	,[YearQuarterID] as year_quarter_id
	,[RequestUserID] as request_user_id
	,[ContractCompletionDate] as contract_completion_date
	,[PlannedCompletionDate] as planned_completion_date
	,[ProjectDuration] as project_duration
	,[CalcWorstCasePrice] as calculated_worst_case_price
	,[CalcBestCasePrice] as calculated_best_case_price
	,CalcRealCasePrice as calculated_real_case_price
	,[IsActive] as is_active
	,[CreatedDate] as created_date
	,[CompanyID] as company_id
	,[ProfitPercent] as profit_percent
	,[Year] as year
	,[ContractPrice] as contract_price,
	is_last_one,
	is_firsttwo_or_lasttwo,
	year_quarter,
	profit,
	profit_tl,
	profit_usd,
	profit_eur,
	profit_rub,

profitpercent_minus_laggedprofitpercent,
profitpercent_over_laggedprofitpercent,
profitpercent_minus_openingprofitpercent,
profitpercent_over_openingprofitpercent,

real_case_price_tl,
real_case_price_usd,
real_case_price_eur,
real_case_price_rub,
profit - lagged_profit as profit_minus_lagged_profit,
profit_tl - lagged_profit_tl as  profit_minus_lagged_profit_tl,
profit_usd -  lagged_profit_usd  as profit_minus_lagged_profit_usd,
profit_eur - lagged_profit_eur as profit_minus_lagged_profit_eur,
profit_rub - lagged_profit_rub as profit_minus_lagged_profit_rub,

(profit - lagged_profit)/NULLIF(lagged_profit,0) as profit_over_lagged_profit,
(profit_tl - lagged_profit_tl)/NULLIF(lagged_profit_tl,0) as profit_over_lagged_profit_tl,
(profit_usd - lagged_profit_usd)/NULLIF(lagged_profit_usd,0) as profit_over_lagged_profit_usd,
(profit_eur - lagged_profit_eur)/NULLIF(lagged_profit_eur,0) as profit_over_lagged_profit_eur,
(profit_rub - lagged_profit_rub)/NULLIF(lagged_profit_rub,0) as profit_over_lagged_profit_rub,


(profit - opening_profit) as profit_minus_opening_profit,
(profit_tl - opening_tl) as profit_minus_opening_profit_tl,
(profit_usd - opening_usd) as profit_minus_opening_profit_usd,
(profit_eur - opening_eur) as profit_minus_opening_profit_eur,
(profit_rub - opening_rub) as profit_minus_opening_profit_rub,


(profit - opening_profit)/NULLIF(opening_profit,0) as profit_over_opening_profit,
(profit_tl - opening_tl)/NULLIF(opening_tl,0) as profit_over_opening_profit_tl,
(profit_usd - opening_usd)/NULLIF(opening_usd,0) as profit_over_opening_profit_usd,
(profit_eur - opening_eur)/NULLIF(opening_eur,0) as profit_over_opening_profit_eur,
(profit_rub - opening_rub)/NULLIF(opening_rub,0) as profit_over_opening_profit_rub,

 profit - CalcRealCasePrice as profit_minus_real_case_price,
 profit_tl - real_case_price_tl as profit_minus_real_case_price_tl,
 profit_usd - real_case_price_usd as profit_minus_real_case_price_usd,
 profit_eur - real_case_price_eur as profit_minus_real_case_price_eur,
 profit_rub - real_case_price_rub as profit_minus_real_case_price_rub,

 lagged_profit - lagged_realcase as laggedprofit_minus_lagged_real_case,
 lagged_profit_tl - lagged_real_case_tl as laggedprofit_minus_lagged_real_case_tl,
 lagged_profit_usd - lagged_real_case_usd as laggedprofit_minus_lagged_real_case_usd,
 lagged_profit_eur - lagged_real_case_eur as laggedprofit_minus_lagged_real_case_eur,
 lagged_profit_rub - lagged_real_case_rub as laggedprofit_minus_lagged_real_case_rub,

 (profit - CalcRealCasePrice) -     (lagged_profit - lagged_realcase) as profit_minus_lagged_realcase,
 (profit_tl - real_case_price_tl) - (lagged_profit_tl - lagged_real_case_tl) as profit_minus_lagged_realcase_tl,
 (profit_usd - real_case_price_usd) - (lagged_profit_usd - lagged_real_case_usd) as profit_minus_lagged_realcase_usd,
 (profit_eur - real_case_price_eur) - (lagged_profit_eur - lagged_real_case_eur) as profit_minus_lagged_realcase_eur,
 (profit_rub - real_case_price_rub) - (lagged_profit_rub - lagged_real_case_rub) as profit_minus_lagged_realcase_rub,

 ((profit - CalcRealCasePrice) - (lagged_profit - lagged_realcase))/NULLIF((lagged_profit - lagged_realcase),0) as profit_over_lagged_realcase,
 ((profit_tl - real_case_price_tl) - (lagged_profit_tl - lagged_real_case_tl))/NULLIF((lagged_profit_tl - lagged_real_case_tl),0) as profit_over_lagged_realcase_tl,
 ((profit_usd - real_case_price_usd) - (lagged_profit_usd - lagged_real_case_usd))/NULLIF((lagged_profit_usd - lagged_real_case_usd),0) as profit_over_lagged_realcase_usd,
 ((profit_eur - real_case_price_eur) - (lagged_profit_eur - lagged_real_case_eur))/NULLIF((lagged_profit_eur - lagged_real_case_eur),0) as profit_over_lagged_realcase_eur,
 ((profit_rub - real_case_price_rub) - (lagged_profit_rub - lagged_real_case_rub))/NULLIF((lagged_profit_rub - lagged_real_case_rub),0) as profit_over_lagged_realcase_rub,

 (profit - CalcRealCasePrice) - (opening_profit - opening_real_case_price) as profit_minus_openingrealcase,
 (profit_tl - real_case_price_tl) - (opening_tl - opening_real_case_tl) as profit_minus_openingrealcase_tl,
 (profit_usd - real_case_price_usd) - (opening_usd - opening_real_case_usd) as profit_minus_openingrealcase_usd,
 (profit_eur - real_case_price_eur) - (opening_eur - opening_real_case_eur) as profit_minus_openingrealcase_eur,
 (profit_rub - real_case_price_rub) - (opening_rub - opening_real_case_rub) as profit_minus_openingrealcase_rub,

 ((profit - CalcRealCasePrice) - (opening_profit - opening_real_case_price))/NULLIF((opening_profit - opening_real_case_price),0) as profit_over_openingprealcase,
 ((profit_tl - real_case_price_tl) - (opening_tl - opening_real_case_tl))/NULLIF((opening_tl - opening_real_case_tl),0)  as profit_over_openingrealcase_tl,
 ((profit_usd - real_case_price_usd) - (opening_usd - opening_real_case_usd))/NULLIF((opening_usd - opening_real_case_usd),0)  as profit_over_openingrealcase_usd,
 ((profit_eur - real_case_price_eur) - (opening_eur - opening_real_case_eur))/NULLIF((opening_eur - opening_real_case_eur),0)  as profit_over_openingrealcase_eur,
 ((profit_rub - real_case_price_rub) - (opening_rub - opening_real_case_rub))/NULLIF((opening_rub - opening_real_case_rub),0)  as profit_over_openingrealcase_rub,

ExpenseBudget,
expense_budget_tl,
expense_budget_usd,
expense_budget_eur,
expense_budget_rub,

ExpenseBudget - lagged_expense_budget as expense_minus_lagged_expense,
expense_budget_tl - lagged_expense_budget_tl as expense_minus_lagged_expense_tl,
expense_budget_usd - lagged_expense_budget_usd as expense_minus_lagged_expense_usd,
expense_budget_eur - lagged_expense_budget_eur as expense_minus_lagged_expense_eur,
expense_budget_rub - lagged_expense_budget_rub as expense_minus_lagged_expense_rub,

(ExpenseBudget      -  lagged_expense_budget)/NULLIF(lagged_expense_budget,0) as expense_over_lagged_expense,
(expense_budget_tl  -  lagged_expense_budget_tl)/NULLIF(lagged_expense_budget_tl,0) as expense_over_lagged_expense_tl,
(expense_budget_usd - lagged_expense_budget_usd)/NULLIF(lagged_expense_budget_usd,0) as expense_over_lagged_expense_usd,
(expense_budget_eur - lagged_expense_budget_eur)/NULLIF(lagged_expense_budget_eur,0) as expense_over_lagged_expense_eur,
(expense_budget_rub - lagged_expense_budget_rub)/NULLIF(lagged_expense_budget_rub,0) as expense_over_lagged_expense_rub,


(ExpenseBudget      -  opening_expensebudget)/NULLIF(opening_expensebudget,0) as expense_over_opening_expense_budget,
(expense_budget_tl  -  opening_expense_budget_tl)/NULLIF(opening_expense_budget_tl,0) as expense_over_opening_expense_budget_tl,
(expense_budget_usd - opening_expense_budget_usd)/NULLIF(opening_expense_budget_usd,0) as expense_over_opening_expense_budget_usd,
(expense_budget_eur - opening_expense_budget_eur)/NULLIF(opening_expense_budget_eur,0) as expense_over_opening_expense_budget_eur,
(expense_budget_rub - opening_expense_budget_rub)/NULLIF(opening_expense_budget_rub,0) as expense_over_opening_expense_budget_rub,

(ExpenseBudget      -  opening_expensebudget) as expense_minus_opening_expense_budget,
(expense_budget_tl  -  opening_expense_budget_tl) as expense_minus_opening_expense_budget_tl,
(expense_budget_usd - opening_expense_budget_usd) as expense_minus_opening_expense_budget_usd,
(expense_budget_eur - opening_expense_budget_eur) as expense_minus_opening_expense_budget_eur,
(expense_budget_rub - opening_expense_budget_rub) as expense_minus_opening_expense_budget_rub,

Revenue as revenue,
revenue_tl as revenue_tl,
revenue_usd as revenue_usd,
revenue_eur as revenue_eur,
revenue_rub as revenue_rub,

revenue-lagged_revenue as revenue_minus_lagged_revenue,
revenue_tl - lagged_revenue_tl as revenue_minus_lagged_revenue_tl, 
revenue_usd - lagged_revenue_usd as revenue_minus_lagged_revenue_usd, 
revenue_eur - lagged_revenue_eur as revenue_minus_lagged_revenue_eur, 
revenue_rub - lagged_revenue_rub as revenue_minus_lagged_revenue_rub, 

(revenue-lagged_revenue)/NULLIF(lagged_revenue,0) as revenue_over_lagged_revenue,
(revenue_tl - lagged_revenue_tl)/NULLIF(lagged_revenue_tl,0) as revenue_over_lagged_revenue_tl, 
(revenue_usd - lagged_revenue_usd)/NULLIF(lagged_revenue_usd,0) as revenue_over_lagged_revenue_usd, 
(revenue_eur - lagged_revenue_eur)/NULLIF(lagged_revenue_eur,0) as revenue_over_lagged_revenue_eur, 
(revenue_rub - lagged_revenue_rub)/NULLIF(lagged_revenue_rub,0) as revenue_over_lagged_revenue_rub, 

Revenue -opening_revenue as revenue_minus_opening_revenue,
(revenue_tl -  opening_revenue_tl ) as revenue_minus_opening_tl,
(revenue_usd - opening_revenue_usd) as revenue_minus_opening_usd,
(revenue_eur - opening_revenue_eur) as revenue_minus_opening_eur,
(revenue_rub - opening_revenue_rub) as revenue_minus_opening_rub,

(Revenue-opening_revenue)/NULLIF(opening_revenue,0) as opening_revenue_over_opening,
(revenue_tl -  opening_revenue_tl)/NULLIF(opening_Revenue_tl,0) as  revenue_over_opening_tl,
(revenue_usd - opening_revenue_usd)/ NULLIF(opening_Revenue_usd,0) as revenue_over_opening_usd ,
(revenue_eur - opening_revenue_eur)/ NULLIF(opening_Revenue_eur,0)  as revenue_over_opening_eur,
(revenue_rub - opening_revenue_rub)/ NULLIF(opening_Revenue_rub,0) as revenue_over_opening_rub,

revenue/NULLIF(ExpenseBudget,0) as revenue_over_cost_ratio,
revenue_tl/NULLIF(expense_budget_tl,0) as revenue_over_cost_ratio_tl,
revenue_usd/NULLIF(expense_budget_usd,0) as revenue_over_cost_ratio_usd,
revenue_eur/NULLIF(expense_budget_eur,0) as revenue_over_cost_ratio_eur,
revenue_rub/NULLIF(expense_budget_rub,0) as revenue_over_cost_ratio_rub,

(revenue/NULLIF(ExpenseBudget,0)) - (lagged_revenue/NULLIF(lagged_expense_budget,0)) as revenue_over_cost_ratio_minus_laggedratio,
(revenue_tl/NULLIF(expense_budget_tl,0)) - (lagged_revenue_tl/NULLIF(lagged_expense_budget_tl,0)) as revenue_over_cost_ratio_minus_laggedratio_tl,
(revenue_usd/NULLIF(expense_budget_usd,0)) - (lagged_revenue_usd/NULLIF(lagged_expense_budget_usd,0)) as revenue_over_cost_ratio_minus_laggedratio_usd,
(revenue_eur/NULLIF(expense_budget_eur,0)) - (lagged_revenue_eur/NULLIF(lagged_expense_budget_eur,0)) as revenue_over_cost_ratio_minus_laggedratio_eur,
(revenue_rub/NULLIF(expense_budget_rub,0))  - (lagged_revenue_rub/NULLIF(lagged_expense_budget_rub,0)) as revenue_over_cost_ratio_minus_laggedratio_rub,

((revenue/NULLIF(ExpenseBudget,0)) - (lagged_revenue/NULLIF(lagged_expense_budget, 0)))/ NULLIF((lagged_revenue/NULLIF(lagged_expense_budget, 0)), 0) as revenue_over_cost_ratio_over_laggedratio,
((revenue_tl/NULLIF(expense_budget_tl,0)) - (lagged_revenue_tl/NULLIF(lagged_expense_budget_tl, 0)))/ NULLIF((lagged_revenue_tl/NULLIF(lagged_expense_budget_tl, 0)), 0) as revenue_over_cost_ratio_over_laggedratio_tl,
((revenue_usd/NULLIF(expense_budget_usd,0)) - (lagged_revenue_usd/NULLIF(lagged_expense_budget_usd, 0)))/ NULLIF((lagged_revenue_usd/NULLIF(lagged_expense_budget_usd, 0)), 0) as revenue_over_cost_ratio_over_laggedratio_usd,
((revenue_eur/NULLIF(expense_budget_eur,0)) - (lagged_revenue_eur/NULLIF(lagged_expense_budget_eur, 0)))/ NULLIF((lagged_revenue_eur/NULLIF(lagged_expense_budget_eur, 0)), 0) as revenue_over_cost_ratio_over_laggedratio_eur,
((revenue_rub/NULLIF(expense_budget_rub,0)) - (lagged_revenue_rub/NULLIF(lagged_expense_budget_rub, 0)))/ NULLIF((lagged_revenue_rub/NULLIF(lagged_expense_budget_rub, 0)), 0) as revenue_over_cost_ratio_over_laggedratio_rub,

((revenue/NULLIF(ExpenseBudget,0)) - (opening_revenue/NULLIF(opening_expensebudget, 0)))/ NULLIF((opening_revenue/NULLIF(opening_expensebudget, 0)), 0) as revenue_over_cost_ratio_over_openingratio,
((revenue_tl/NULLIF(expense_budget_tl,0)) - (opening_revenue_tl/NULLIF(opening_expense_budget_tl, 0)))/ NULLIF((opening_revenue_tl/NULLIF(opening_expense_budget_tl, 0)), 0) as revenue_over_cost_ratio_over_openingratio_tl,
((revenue_usd/NULLIF(expense_budget_usd,0)) - (opening_revenue_usd/NULLIF(opening_expense_budget_usd, 0)))/ NULLIF((opening_revenue_usd/NULLIF(opening_expense_budget_usd, 0)), 0) as revenue_over_cost_ratio_over_openingratio_usd,
((revenue_eur/NULLIF(expense_budget_eur,0)) - (opening_revenue_eur/NULLIF(opening_expense_budget_eur, 0)))/ NULLIF((opening_revenue_eur/NULLIF(opening_expense_budget_eur, 0)), 0) as revenue_over_cost_ratio_over_openingratio_eur,
((revenue_rub/NULLIF(expense_budget_rub,0)) - (opening_revenue_rub/NULLIF(opening_expense_budget_rub, 0)))/ NULLIF((opening_revenue_rub/NULLIF(opening_expense_budget_rub, 0)), 0) as revenue_over_cost_ratio_over_openingratio_rub,

((revenue/NULLIF(ExpenseBudget,0)) - (opening_revenue/NULLIF(opening_expensebudget, 0))) as revenue_over_cost_ratio_minus_openingratio,
((revenue_tl/NULLIF(expense_budget_tl,0)) - (opening_revenue_tl/NULLIF(opening_expense_budget_tl, 0))) as revenue_over_cost_ratio_minus_openingratio_tl,
((revenue_usd/NULLIF(expense_budget_usd,0)) - (opening_revenue_usd/NULLIF(opening_expense_budget_usd, 0))) as revenue_over_cost_ratio_minus_openingratio_usd,
((revenue_eur/NULLIF(expense_budget_eur,0)) - (opening_revenue_eur/NULLIF(opening_expense_budget_eur, 0))) as revenue_over_cost_ratio_minus_openingratio_eur,
((revenue_rub/NULLIF(expense_budget_rub,0)) - (opening_revenue_rub/NULLIF(opening_expense_budget_rub, 0))) as revenue_over_cost_ratio_minus_openingratio_rub,

tl,
usd,
eur,
rub

from final_cte

)
,final_cte_3  as (
	
SELECT
f.*,
case when is_last_one = '1' then profit_tl else null end as profit_last_tl,
case when is_last_one = '1' then profit_usd else null end as profit_last_usd,
case when is_last_one = '1' then profit_eur else null end as profit_last_eur,
case when is_last_one = '1' then profit_rub else null end as profit_last_rub,

case when is_last_one = '1' then calculated_worst_case_price*tl else null end as worstcaseprice_last_tl,
case when is_last_one = '1' then calculated_worst_case_price*usd else null end as worstcaseprice_last_usd,
case when is_last_one = '1' then calculated_worst_case_price*eur else null end as worstcaseprice_last_eur,
case when is_last_one = '1' then calculated_worst_case_price*rub else null end as worstcaseprice_last_rub,

case when is_last_one = '1' then calculated_best_case_price*tl else null end as bestcaseprice_last_tl,
case when is_last_one = '1' then calculated_best_case_price*usd else null end as bestcaseprice_last_usd,
case when is_last_one = '1' then calculated_best_case_price*eur else null end as bestcaseprice_last_eur,
case when is_last_one = '1' then calculated_best_case_price*rub else null end as bestcaseprice_last_rub,

case when is_last_one = '1' then calculated_real_case_price*tl else null end as realcaseprice_last_tl,
case when is_last_one = '1' then calculated_real_case_price*usd else null end as realcasecaseprice_last_usd,
case when is_last_one = '1' then calculated_real_case_price*eur else null end as realcaseprice_last_eur,
case when is_last_one = '1' then calculated_real_case_price*rub else null end as realcaseprice_last_rub,

calculated_worst_case_price*tl as   worst_case_price_tl,
calculated_worst_case_price*usd as   worst_case_price_usd,
calculated_worst_case_price*eur as   worst_case_price_eur,
calculated_worst_case_price*rub as   worst_case_price_rub,

calculated_best_case_price*tl  as    best_case_price_tl,
calculated_best_case_price*usd as   best_case_price_usd,
calculated_best_case_price*eur as   best_case_price_eur,
calculated_best_case_price*rub as   best_case_price_rub

from final_cte_2 f
)

, final_cte3 as (
SELECT
m.*,
m.profit_last_tl - worstcaseprice_last_tl as last_profit_minus_last_worstcaseprice_tl,
m.profit_last_usd - worstcaseprice_last_usd as last_profit_minus_last_worstcaseprice_usd,
m.profit_last_eur - worstcaseprice_last_eur as last_profit_minus_last_worstcaseprice_eur,
m.profit_last_rub - worstcaseprice_last_rub as last_profit_minus_last_worstcaseprice_rub,

m.profit_last_tl - realcaseprice_last_tl as last_profit_minus_last_realcaseprice_tl,
m.profit_last_usd - realcasecaseprice_last_usd as last_profit_minus_last_realcaseprice_usd,
m.profit_last_eur - realcaseprice_last_eur as last_profit_minus_last_realcaseprice_eur,
m.profit_last_rub - realcaseprice_last_rub as last_profit_minus_last_realcaseprice_rub,

m.profit_last_tl -  bestcaseprice_last_tl as last_profit_minus_last_bestcaseprice_tl,
m.profit_last_usd - bestcaseprice_last_usd as last_profit_minus_last_bestcaseprice_usd,
m.profit_last_eur - bestcaseprice_last_eur as last_profit_minus_last_bestcaseprice_eur,
m.profit_last_rub - bestcaseprice_last_rub as last_profit_minus_last_bestcaseprice_rub,

(profit_tl - best_case_price_tl) as profit_minus_bestcaseprice_tl,
(profit_eur - best_case_price_eur) as profit_minus_bestcaseprice_eur,
(profit_usd - best_case_price_usd) as profit_minus_bestcaseprice_usd,
(profit_rub - best_case_price_rub) as profit_minus_bestcaseprice_rub,

(profit_tl -  worst_case_price_tl) as profit_minus_worstcaseprice_tl,
(profit_eur - worst_case_price_eur) as profit_minus_worstcaseprice_eur,
(profit_usd - worst_case_price_usd) as profit_minus_worstcaseprice_usd,
(profit_rub - worst_case_price_rub) as profit_minus_worstcaseprice_rub,
rp.Code as project_code
FROM final_cte_3 m
	left join PRODAPPSDB.RNS_RISK_PROD.[dbo].[PRJ_PROJECTS] rp on rp.ObjectID = m.project_id and rp.IsDeleted = '0' and rp.IsActive = '1'
)

SELECT [object_id]
      ,[code]
      ,[status_id]
      ,[project_id]
      ,[risk_unit_id]
      ,[template_process_id]
      ,[risk_impact_id]
      ,[risk_group_id]
      ,[year_quarter_id]
      ,[request_user_id]
      ,[contract_completion_date]
      ,[planned_completion_date]
      ,[project_duration]
      ,[calculated_worst_case_price]
      ,[calculated_best_case_price]
      ,[calculated_real_case_price]
      ,[is_active]
      ,[created_date]
      ,[company_id]
      ,[profit_percent]
      ,[year]
      ,[contract_price]
      ,[is_last_one]
      ,[is_firsttwo_or_lasttwo]
      ,[year_quarter]
      ,[profit]
      ,[profit_tl]
      ,[profit_usd]
      ,[profit_eur]
      ,[profit_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profitpercent_minus_laggedprofitpercent]   END AS [profitpercent_minus_laggedprofitpercent]  
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profitpercent_over_laggedprofitpercent]	  END AS [profitpercent_over_laggedprofitpercent]	 
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profitpercent_minus_openingprofitpercent]  END AS [profitpercent_minus_openingprofitpercent] 
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profitpercent_over_openingprofitpercent]	  END AS [profitpercent_over_openingprofitpercent]	 
      ,[real_case_price_tl]
      ,[real_case_price_usd]
      ,[real_case_price_eur]
      ,[real_case_price_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_lagged_profit]                             END AS   [profit_minus_lagged_profit]                   
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_lagged_profit_tl]						  END AS   [profit_minus_lagged_profit_tl]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_lagged_profit_usd]						  END AS   [profit_minus_lagged_profit_usd]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_lagged_profit_eur]						  END AS   [profit_minus_lagged_profit_eur]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_lagged_profit_rub]						  END AS   [profit_minus_lagged_profit_rub]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_lagged_profit]							  END AS   [profit_over_lagged_profit]					
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_lagged_profit_tl]							  END AS   [profit_over_lagged_profit_tl]					
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_lagged_profit_usd]						  END AS   [profit_over_lagged_profit_usd]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_lagged_profit_eur]						  END AS   [profit_over_lagged_profit_eur]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_lagged_profit_rub]						  END AS   [profit_over_lagged_profit_rub]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_opening_profit]							  END AS   [profit_minus_opening_profit]					
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_opening_profit_tl]						  END AS   [profit_minus_opening_profit_tl]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_opening_profit_usd]						  END AS   [profit_minus_opening_profit_usd]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_opening_profit_eur]						  END AS   [profit_minus_opening_profit_eur]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_opening_profit_rub]						  END AS   [profit_minus_opening_profit_rub]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_opening_profit]							  END AS   [profit_over_opening_profit]					
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_opening_profit_tl]						  END AS   [profit_over_opening_profit_tl]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_opening_profit_usd]						  END AS   [profit_over_opening_profit_usd]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_opening_profit_eur]						  END AS   [profit_over_opening_profit_eur]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_opening_profit_rub]						  END AS   [profit_over_opening_profit_rub]				
      ,[profit_minus_real_case_price]					
      ,[profit_minus_real_case_price_tl]				
      ,[profit_minus_real_case_price_usd]				
      ,[profit_minus_real_case_price_eur]				
      ,[profit_minus_real_case_price_rub]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [laggedprofit_minus_lagged_real_case]					  END AS   [laggedprofit_minus_lagged_real_case]			
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [laggedprofit_minus_lagged_real_case_tl]				  END AS   [laggedprofit_minus_lagged_real_case_tl]		
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [laggedprofit_minus_lagged_real_case_usd]				  END AS   [laggedprofit_minus_lagged_real_case_usd]		
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [laggedprofit_minus_lagged_real_case_eur]				  END AS   [laggedprofit_minus_lagged_real_case_eur]		
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [laggedprofit_minus_lagged_real_case_rub]				  END AS   [laggedprofit_minus_lagged_real_case_rub]		
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_lagged_realcase]							  END AS   [profit_minus_lagged_realcase]					
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_lagged_realcase_tl]						  END AS   [profit_minus_lagged_realcase_tl]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_lagged_realcase_usd]						  END AS   [profit_minus_lagged_realcase_usd]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_lagged_realcase_eur]						  END AS   [profit_minus_lagged_realcase_eur]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_lagged_realcase_rub]						  END AS   [profit_minus_lagged_realcase_rub]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_lagged_realcase]							  END AS   [profit_over_lagged_realcase]					
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_lagged_realcase_tl]						  END AS   [profit_over_lagged_realcase_tl]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_lagged_realcase_usd]						  END AS   [profit_over_lagged_realcase_usd]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_lagged_realcase_eur]						  END AS   [profit_over_lagged_realcase_eur]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_lagged_realcase_rub]						  END AS   [profit_over_lagged_realcase_rub]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_openingrealcase]						      END AS 	[profit_minus_openingrealcase]		  																
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_openingrealcase_tl]						  END AS 	[profit_minus_openingrealcase_tl]						
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_openingrealcase_usd]						  END AS 	[profit_minus_openingrealcase_usd]	
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_openingrealcase_eur]						  END AS 	[profit_minus_openingrealcase_eur]	
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_minus_openingrealcase_rub]						  END AS 	[profit_minus_openingrealcase_rub]	
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_openingprealcase]							  END AS 	[profit_over_openingprealcase]		
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_openingrealcase_tl]						  END AS 	[profit_over_openingrealcase_tl]	
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_openingrealcase_usd]						  END AS 	[profit_over_openingrealcase_usd]	
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_openingrealcase_eur]						  END AS 	[profit_over_openingrealcase_eur]	
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [profit_over_openingrealcase_rub]						  END AS 	[profit_over_openingrealcase_rub]	
      ,[ExpenseBudget]
      ,[expense_budget_tl]
      ,[expense_budget_usd]
      ,[expense_budget_eur]
      ,[expense_budget_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_minus_lagged_expense]                   END AS [expense_minus_lagged_expense]            
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_minus_lagged_expense_tl]				   END AS [expense_minus_lagged_expense_tl]					
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_minus_lagged_expense_usd]			   END AS [expense_minus_lagged_expense_usd]		
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_minus_lagged_expense_eur]			   END AS [expense_minus_lagged_expense_eur]		
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_minus_lagged_expense_rub]			   END AS [expense_minus_lagged_expense_rub]		
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_over_lagged_expense]					   END AS [expense_over_lagged_expense]				
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_over_lagged_expense_tl]				   END AS [expense_over_lagged_expense_tl]			
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_over_lagged_expense_usd]				   END AS [expense_over_lagged_expense_usd]			
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_over_lagged_expense_eur]				   END AS [expense_over_lagged_expense_eur]			
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_over_lagged_expense_rub]				   END AS [expense_over_lagged_expense_rub]			
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_over_opening_expense_budget]			   END AS [expense_over_opening_expense_budget]		
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_over_opening_expense_budget_tl]		   END AS [expense_over_opening_expense_budget_tl]	
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_over_opening_expense_budget_usd]		   END AS [expense_over_opening_expense_budget_usd]	
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_over_opening_expense_budget_eur]		   END AS [expense_over_opening_expense_budget_eur]	
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_over_opening_expense_budget_rub]		   END AS [expense_over_opening_expense_budget_rub]	
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_minus_opening_expense_budget]		   END AS [expense_minus_opening_expense_budget]	
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_minus_opening_expense_budget_tl]		   END AS [expense_minus_opening_expense_budget_tl]	
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_minus_opening_expense_budget_usd]	   END AS [expense_minus_opening_expense_budget_usd]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_minus_opening_expense_budget_eur]	   END AS [expense_minus_opening_expense_budget_eur]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE  [expense_minus_opening_expense_budget_rub]	   END AS [expense_minus_opening_expense_budget_rub]
      ,[revenue]
      ,[revenue_tl]
      ,[revenue_usd]
      ,[revenue_eur]
      ,[revenue_rub]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_minus_lagged_revenue]                      END AS [revenue_minus_lagged_revenue]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_minus_lagged_revenue_tl]					END AS [revenue_minus_lagged_revenue_tl]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_minus_lagged_revenue_usd]					END AS [revenue_minus_lagged_revenue_usd]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_minus_lagged_revenue_eur]					END AS [revenue_minus_lagged_revenue_eur]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_minus_lagged_revenue_rub]					END AS [revenue_minus_lagged_revenue_rub]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_lagged_revenue]						END AS [revenue_over_lagged_revenue]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_lagged_revenue_tl]					END AS [revenue_over_lagged_revenue_tl]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_lagged_revenue_usd]					END AS [revenue_over_lagged_revenue_usd]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_lagged_revenue_eur]					END AS [revenue_over_lagged_revenue_eur]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_lagged_revenue_rub]					END AS [revenue_over_lagged_revenue_rub]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_minus_opening_revenue]						END AS [revenue_minus_opening_revenue]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_minus_opening_tl]							END AS [revenue_minus_opening_tl]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_minus_opening_usd]							END AS [revenue_minus_opening_usd]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_minus_opening_eur]							END AS [revenue_minus_opening_eur]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_minus_opening_rub]							END AS [revenue_minus_opening_rub]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [opening_revenue_over_opening]						END AS [opening_revenue_over_opening]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_opening_tl]							END AS [revenue_over_opening_tl]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_opening_usd]							END AS [revenue_over_opening_usd]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_opening_eur]							END AS [revenue_over_opening_eur]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_opening_rub]							END AS [revenue_over_opening_rub]
     ,[revenue_over_cost_ratio]
     ,[revenue_over_cost_ratio_tl]
     ,[revenue_over_cost_ratio_usd]
     ,[revenue_over_cost_ratio_eur]
     ,[revenue_over_cost_ratio_rub]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_minus_laggedratio]			END AS [revenue_over_cost_ratio_minus_laggedratio]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_minus_laggedratio_tl]		END AS [revenue_over_cost_ratio_minus_laggedratio_tl]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_minus_laggedratio_usd]		END AS [revenue_over_cost_ratio_minus_laggedratio_usd]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_minus_laggedratio_eur]		END AS [revenue_over_cost_ratio_minus_laggedratio_eur]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_minus_laggedratio_rub]		END AS [revenue_over_cost_ratio_minus_laggedratio_rub]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_over_laggedratio]			END AS [revenue_over_cost_ratio_over_laggedratio]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_over_laggedratio_tl]		END AS [revenue_over_cost_ratio_over_laggedratio_tl]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_over_laggedratio_usd]		END AS [revenue_over_cost_ratio_over_laggedratio_usd]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_over_laggedratio_eur]		END AS [revenue_over_cost_ratio_over_laggedratio_eur]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_over_laggedratio_rub]		END AS [revenue_over_cost_ratio_over_laggedratio_rub]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_over_openingratio]			END AS [revenue_over_cost_ratio_over_openingratio]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_over_openingratio_tl]		END AS [revenue_over_cost_ratio_over_openingratio_tl]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_over_openingratio_usd]		END AS [revenue_over_cost_ratio_over_openingratio_usd]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_over_openingratio_eur]		END AS [revenue_over_cost_ratio_over_openingratio_eur]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_over_openingratio_rub]		END AS [revenue_over_cost_ratio_over_openingratio_rub]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_minus_openingratio]		END AS [revenue_over_cost_ratio_minus_openingratio]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_minus_openingratio_tl]		END AS [revenue_over_cost_ratio_minus_openingratio_tl]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_minus_openingratio_usd]	END AS [revenue_over_cost_ratio_minus_openingratio_usd]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_minus_openingratio_eur]	END AS [revenue_over_cost_ratio_minus_openingratio_eur]
     ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [revenue_over_cost_ratio_minus_openingratio_rub]	END AS [revenue_over_cost_ratio_minus_openingratio_rub]
      ,[tl]
      ,[usd]
      ,[eur]
      ,[rub]
      ,[profit_last_tl]
      ,[profit_last_usd]
      ,[profit_last_eur]
      ,[profit_last_rub]
      ,[worstcaseprice_last_tl]
      ,[worstcaseprice_last_usd]
      ,[worstcaseprice_last_eur]
      ,[worstcaseprice_last_rub]
      ,[bestcaseprice_last_tl]
      ,[bestcaseprice_last_usd]
      ,[bestcaseprice_last_eur]
      ,[bestcaseprice_last_rub]
      ,[realcaseprice_last_tl]
      ,[realcasecaseprice_last_usd]
      ,[realcaseprice_last_eur]
      ,[realcaseprice_last_rub]
      ,[worst_case_price_tl]
      ,[worst_case_price_usd]
      ,[worst_case_price_eur]
      ,[worst_case_price_rub]
      ,[best_case_price_tl]
      ,[best_case_price_usd]
      ,[best_case_price_eur]
      ,[best_case_price_rub]
      ,[last_profit_minus_last_worstcaseprice_tl]
      ,[last_profit_minus_last_worstcaseprice_usd]
      ,[last_profit_minus_last_worstcaseprice_eur]
      ,[last_profit_minus_last_worstcaseprice_rub]
      ,[last_profit_minus_last_realcaseprice_tl]
      ,[last_profit_minus_last_realcaseprice_usd]
      ,[last_profit_minus_last_realcaseprice_eur]
      ,[last_profit_minus_last_realcaseprice_rub]
      ,[last_profit_minus_last_bestcaseprice_tl]
      ,[last_profit_minus_last_bestcaseprice_usd]
      ,[last_profit_minus_last_bestcaseprice_eur]
      ,[last_profit_minus_last_bestcaseprice_rub]
      ,[profit_minus_bestcaseprice_tl]
      ,[profit_minus_bestcaseprice_eur]
      ,[profit_minus_bestcaseprice_usd]
      ,[profit_minus_bestcaseprice_rub]
      ,[profit_minus_worstcaseprice_tl]
      ,[profit_minus_worstcaseprice_eur]
      ,[profit_minus_worstcaseprice_usd]
      ,[profit_minus_worstcaseprice_rub]
      ,[project_code]
  FROM final_cte3
