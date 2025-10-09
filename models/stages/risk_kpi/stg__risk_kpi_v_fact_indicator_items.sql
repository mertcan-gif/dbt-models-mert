{{
  config(
    materialized = 'table',tags = ['risk_kpi']
    )
}}
WITH x as (
SELECT
case
	when YearQuarterID = '1' THEN CONCAT('Tender', i.Year )
	when YearQuarterID = '2' THEN CONCAT('Opening',  i.Year )
	when YearQuarterID = '3' THEN CONCAT(i.Year, '-', 'Q1' )
	when YearQuarterID = '4' THEN CONCAT(i.Year,'-','Q2')
	when YearQuarterID = '5' THEN CONCAT(i.Year,'-','Q3')
	when YearQuarterID = '6' THEN CONCAT(i.Year,'-','Q4')
ELSE NULL END AS  year_quarter
      ,i.[YearQuarterID] as year_quarter_id
      ,i.[RequestUserID] as request_user_id
      ,i.[PrincibleType] as principle_type
      ,i.[CutOffDate] as cutoff_date
      ,i.[ContractCompletionDate] as contract_completion_date
      ,i.[PlannedCompletionDate] as planned_completion_date
      ,i.[ProjectDuration] as project_duration
      ,i.[Revenue] as revenue
      ,i.[ExpenseBudget] as expense_budget
      ,i.[ContractPrice] as contract_price
      ,i.[Profit] as profit
      ,i.[ProfitPercent] as profit_percent
      ,i.[RiskBudget] as risk_budget
      ,i.[Cpi] as cpi 
      ,i.[Spi] as spi
      ,i.[BudgetClaims] as budget_claims
      ,i.[ChangeOrder] as change_order
      ,i.[RealizedCashIn] as realized_cash_in
      ,i.[RealizedCashOut] as realized_cash_out
,i.code
,p.name as project_name
,p.Code as project_code
,i.ProjectID as project_id
,ie.ItemKeyName as itemkeyname
,case  
	WHEN DENSE_RANK() OVER (
    PARTITION BY p.Name
    ORDER BY Year DESC, 
            YearQuarterID desc
	)<=1 THEN '1' 
	else null
	end as  is_last_one,
	ic.Value as tl,
	ic1.Value as usd,
	ic2.Value as eur,
	ic3.Value as rub,

	ic.Value*ie.Value as indicator_value_tl,
	ic1.Value*ie.Value as indicator_value_usd,
	ic2.Value*ie.Value as indicator_value_eur,
	ic3.Value*ie.Value as indicator_value_rub,
	ic4.DisplayName as indicator_transaction_currency
FROM PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATORS] i
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].PRJ_PROJECTS p on p.ObjectID = i.ProjectID
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].IND_INDICATOR_ITEMS  ie on ie.IndicatorID = i.ObjectID
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATOR_CURRENCIES] ic on ic.IndicatorID = i.ObjectID and ic.DisplayName = 'TL'  and ic.IsActive  = '1' and ic.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATOR_CURRENCIES] ic1 on ic1.IndicatorID = i.ObjectID and ic1.DisplayName = 'USD' and ic1.IsActive  = '1' and ic1.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATOR_CURRENCIES] ic2 on ic2.IndicatorID = i.ObjectID and ic2.DisplayName = 'EUR' and ic2.IsActive  = '1' and ic2.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATOR_CURRENCIES] ic3 on ic3.IndicatorID = i.ObjectID and ic3.DisplayName = 'RUB' and ic3.IsActive  = '1' and ic3.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATOR_CURRENCIES] ic4 on ic4.IndicatorID = i.ObjectID and ic4.IsPrimary = '1' and ic4.IsDeleted = '0'
WHERE 1=1
	and i.IsDeleted = '0'
	and i.IsActive = '1'
	and ie.IsDeleted = '0'
	and ie.IsActive = '1'
	and p.IsDeleted = '0'
	and p.IsActive = '1'
)
SELECT
x.*,
case when is_last_one= '1' then indicator_value_tl end as indicator_last_value_tl,
case when is_last_one= '1' then indicator_value_usd end as indicator_last_value_usd,
case when is_last_one= '1' then indicator_value_eur end as indicator_last_value_eur,
case when is_last_one= '1' then indicator_value_rub end as indicator_last_value_rub
FROM x 