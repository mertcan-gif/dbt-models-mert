{{
  config(
    materialized = 'table',tags = ['risk_kpi']
    )
}}
SELECT DISTINCT
	p.Name as project_name,
	p.ObjectID as project_id,
	p.Code as code,
	df.name as companyname,
	ic.Value as tl,
	ic1.Value as usd,
	ic2.Value as eur,
	ic3.Value as rub,
	Revenue*ic.Value AS revenue_tl,
	Revenue*ic1.Value as revenue_usd,
	Revenue*ic2.Value as revenue_eur,
	Revenue*ic3.Value as revenue_rub,
	Profit*ic.Value AS profit_tl,
	Profit*ic1.Value as profit_usd,
	Profit*ic2.Value as profit_eur,
	Profit*ic3.Value as profit_rub,
	ExpenseBudget*ic.Value AS expense_tl,
	ExpenseBudget*ic1.Value as expense_usd,
	ExpenseBudget*ic2.Value as expense_eur,
	ExpenseBudget*ic3.Value as expense_rub
FROM PRODAPPSDB.RNS_RISK_PROD.[dbo].PRJ_PROJECTS p
LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[DEF_COMPANIES] df on df.ObjectID = p.CompanyID
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[PRJ_PROJECT_CURRENCIES] ic  on ic.ProjectID  = p.ObjectID and ic.DisplayName = 'TL'  and ic.IsActive  = '1' and ic.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[PRJ_PROJECT_CURRENCIES] ic1 on ic1.ProjectID = p.ObjectID and ic1.DisplayName = 'USD' and ic1.IsActive  = '1' and ic1.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[PRJ_PROJECT_CURRENCIES] ic2 on ic2.ProjectID = p.ObjectID and ic2.DisplayName = 'EUR' and ic2.IsActive  = '1' and ic2.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[PRJ_PROJECT_CURRENCIES] ic3 on ic3.ProjectID = p.ObjectID and ic3.DisplayName = 'RUB' and ic3.IsActive  = '1' and ic3.IsDeleted = '0'
where 1=1
    and p.IsDeleted = '0'
    and p.IsActive = '1'
	AND df.IsDeleted = '0'
	AND df.IsActive  = '1'
