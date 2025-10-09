{{
  config(
    materialized = 'table',tags = ['risk_kpi']
    )
}}
WITH x AS (
SELECT
i.*,
case
	when i.YearQuarterID = '1' THEN CONCAT('Tender', i.Year )
	when i.YearQuarterID = '2' THEN CONCAT('Opening',  i.Year )
	when i.YearQuarterID = '3' THEN CONCAT(i.Year,'-','Q1')
	when i.YearQuarterID = '4' THEN CONCAT(i.Year,'-','Q2')
	when i.YearQuarterID = '5' THEN CONCAT(i.Year,'-','Q3')
	when i.YearQuarterID = '6' THEN CONCAT(i.Year,'-','Q4')
ELSE NULL END AS  year_quarter,
cast(ic.Value as DECIMAL(20,10)) as tl,
cast(ic1.Value as DECIMAL(20,10)) as usd,
cast(ic2.Value as DECIMAL(20,10)) as eur,
cast(ic3.Value as DECIMAL(20,10)) as rub,
LAG(cast(ic.Value as DECIMAL(20,10))) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ) as lagged_tl,
LAG(cast(ic1.Value as DECIMAL(20,10))) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ) as lagged_usd,
LAG(cast(ic2.Value as DECIMAL(20,10))) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ) as lagged_eur,
LAG(cast(ic3.Value as DECIMAL(20,10))) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ) as lagged_rub,

LAG(i.spi) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ) as lagged_spi,
(i.spi- LAG(i.spi) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC )) as spi_minus_lagged_spi,
(i.spi- LAG(i.spi) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC )) /NULLIF((LAG(i.spi) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC )),0) as spi_over_laggedspi,
(i.spi- op.spi) as spi_minus_opening_spi,
(i.spi- op.spi)/NULLIF(op.spi,0) as spi_over_opening_spi,
LAG(i.cpi) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ) as lagged_cpi,
(i.cpi- LAG(i.cpi) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC )) as cpi_minus_lagged_cpi,
(i.cpi- LAG(i.cpi) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ))/NULLIF((LAG(i.cpi) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC )),0) as cpi_over_laggedcpi,
(i.cpi- op.cpi) as cpi_minus_openingcpi,
(i.cpi- op.cpi)/NULLIF((op.cpi),0) as cpi_over_openingcpi,

LAG(i.ChangeOrder) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ) as lagged_changeorder,
op.ChangeOrder as opening_changeorder,
i.ChangeOrder/NULLIF(i.Revenue,0) as changeorder_over_revenue,
(i.ChangeOrder/NULLIF(i.Revenue,0))-((LAG(i.ChangeOrder) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ))/(NULLIF(LAG(i.Revenue) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC),0))) as changeorderrevenue_minus_previouschangeorderrevenue,
((i.ChangeOrder/NULLIF(i.Revenue,0))-((LAG(i.ChangeOrder) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ))/(NULLIF(LAG(i.Revenue) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC),0)))) / NULLIF(((LAG(i.ChangeOrder) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ))/(NULLIF(LAG(i.Revenue) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC),0))),0)
 as changeorderrevenue_over_previouschangeorderrevenue,
 (i.ChangeOrder/NULLIF(i.Revenue,0))-(op.ChangeOrder/NULLIF(op.Revenue,0)) as changeorderrevenue_minus_openingchangeorderrevenue,
 (  (i.ChangeOrder/NULLIF(i.Revenue,0))-(op.ChangeOrder/NULLIF(op.Revenue,0)) ) / NULLIF((op.ChangeOrder/NULLIF(op.Revenue,0)),0)   as changeorderrevenue_over_openingchangeorderrevenue,


(i.BudgetClaims/NULLIF(i.Revenue,0)) as budget_over_revenue,
(i.BudgetClaims/NULLIF(i.Revenue,0))-(LAG(i.BudgetClaims) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC )/NULLIF(LAG(i.Revenue) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ),0)) as claimrevenue_minus_previousclaimrevenue,
((i.BudgetClaims/NULLIF(i.Revenue,0))-(LAG(i.BudgetClaims) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC )/NULLIF(LAG(i.Revenue) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ),0)))/(NULLIF(LAG(i.BudgetClaims) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ),0)/NULLIF(LAG(i.Revenue) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ),0)) as claimrevenue_over_previousclaimrevenue,
(i.BudgetClaims/NULLIF(i.Revenue,0))-(op.BudgetClaims/NULLIF(op.Revenue,0)) as claimrevenue_minus_openingclaimrevenue,
((i.BudgetClaims/NULLIF(i.Revenue,0))-(op.BudgetClaims/NULLIF(op.Revenue,0)))/(NULLIF(op.BudgetClaims,0)/NULLIF(op.Revenue,0)) as claimrevenue_over_openingclaimrevenue,
LAG(i.BudgetClaims) Over(partition by i.ProjectID Order by i.Year,i.YearQuarterID ASC ) as lagged_budgetclaims,


LAG(i.Revenue) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ) as lagged_revenue,
op.Revenue as opening_revenue,
cast(op.tl as DECIMAL(20,10)) as opening_quarter_tl,
cast(op.eur as DECIMAL(20,10)) as opening_quarter_eur,
cast(op.usd as DECIMAL(20,10)) as opening_quarter_usd,
cast(op.rub as DECIMAL(20,10)) as opening_quarter_rub,

op.RealizedCashIn as opening_cashin,
op.RealizedCashOut as opening_cashout,
case  
	WHEN DENSE_RANK() OVER (
    PARTITION BY p.Name
    ORDER BY i.Year DESC, 
            i.YearQuarterID desc
	)<=1 THEN '1' 
	else null
	end as  is_last_one,
op.BudgetClaims as opening_budget_claims,

LAG(i.RealizedCashIn) Over(PARTITION BY i.ProjectID Order by i.Year,i.YearQuarterID ASC ) as lagged_realizedcashin,
LAG(i.RealizedCashOut) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ) as lagged_realizedcashout,
i.RealizedCashIn-i.RealizedCashOut as net_cash,
(LAG(i.RealizedCashIn) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC ) - LAG(i.RealizedCashOut) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC )) as lagged_realized_net_cash,
(i.RealizedCashIn-i.RealizedCashOut ) -  (LAG(i.RealizedCashIn) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC )- LAG(i.RealizedCashOut) Over(PARTITION by i.ProjectID Order by i.Year,i.YearQuarterID ASC )) AS netcash_minus_previousnetcash,
    (i.RealizedCashIn - i.RealizedCashOut) -
    ((i.RealizedCashIn - i.RealizedCashOut) /
    NULLIF(
        (LAG(i.RealizedCashIn) OVER (PARTITION BY i.ProjectID ORDER BY i.Year, i.YearQuarterID ASC) 
        - LAG(i.RealizedCashOut) OVER (PARTITION BY i.ProjectID ORDER BY i.Year, i.YearQuarterID ASC)), 0)
    ) AS netcash_over_previousnetcash,
(i.RealizedCashIn-i.RealizedCashOut)-(op.RealizedCashIn-op.RealizedCashOut) AS netcash_minus_openingnetcash,
((i.RealizedCashIn-i.RealizedCashOut)-(op.RealizedCashIn-op.RealizedCashOut))/NULLIF((op.RealizedCashIn-op.RealizedCashOut),0) AS netcash_over_openingnetcash,
imain.DisplayName,
p.Code as project_code
FROM PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATORS] i
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].PRJ_PROJECTS p on p.ObjectID = i.ProjectID
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATOR_CURRENCIES] ic on ic.IndicatorID = i.ObjectID and ic.DisplayName = 'TL'  and ic.IsActive  = '1' and ic.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATOR_CURRENCIES] ic1 on ic1.IndicatorID = i.ObjectID and ic1.DisplayName = 'USD' and ic1.IsActive  = '1' and ic1.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATOR_CURRENCIES] ic2 on ic2.IndicatorID = i.ObjectID and ic2.DisplayName = 'EUR' and ic2.IsActive  = '1' and ic2.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATOR_CURRENCIES] ic3 on ic3.IndicatorID = i.ObjectID and ic3.DisplayName = 'RUB' and ic3.IsActive  = '1' and ic3.IsDeleted = '0'
	LEFT JOIN 
				(select
			m.*,
			ic.Value as tl,
			ic1.Value as usd,
			ic2.Value as eur,
			ic3.Value as rub
			from PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATORS] m
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATOR_CURRENCIES] ic on ic.IndicatorID = m.ObjectID and ic.DisplayName = 'TL'  and ic.IsActive  = '1' and ic.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATOR_CURRENCIES] ic1 on ic1.IndicatorID = m.ObjectID and ic1.DisplayName = 'USD' and ic1.IsActive  = '1' and ic1.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATOR_CURRENCIES] ic2 on ic2.IndicatorID = m.ObjectID and ic2.DisplayName = 'EUR' and ic2.IsActive  = '1' and ic2.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATOR_CURRENCIES] ic3 on ic3.IndicatorID = m.ObjectID and ic3.DisplayName = 'RUB' and ic3.IsActive  = '1' and ic3.IsDeleted = '0'
			where 1=1
			 and m.IsActive = '1'
			 and m.IsDeleted = '0'
			 and m.YearQuarterID = '2') as  op on op.ProjectID = i.ProjectID
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[IND_INDICATOR_CURRENCIES] imain on imain.IndicatorID = i.ObjectID and imain.IsPrimary = '1'
WHERE 1=1
	and i.IsDeleted = '0'
	and i.IsActive = '1'
	and p.IsDeleted = '0'
	and p.IsActive = '1'
), final_cte as (
SELECT
 [ObjectID]
,[RiskUnitID]
,[TemplateProcessID]
,[Code]
,[Year]
,[ProjectID]
,[YearQuarterID]
,[RequestUserID]
,[PrincibleType]
,[CutOffDate]
,[ContractCompletionDate]
,[PlannedCompletionDate]
,[ProjectDuration]
,year_quarter,

ChangeOrder*tl as changeorder_tl,
ChangeOrder*eur as changeorder_eur,
ChangeOrder*usd as changeorder_usd,
ChangeOrder*rub as changeorder_rub,
lagged_changeorder*lagged_tl as lagged_change_order_tl,
lagged_changeorder*lagged_eur as lagged_change_order_eur,
lagged_changeorder*lagged_usd as lagged_change_order_usd,
lagged_changeorder*lagged_rub as lagged_change_order_rub,
opening_changeorder*opening_quarter_tl as opening_changeorder_tl,
opening_changeorder*opening_quarter_eur as opening_changeorder_eur,
opening_changeorder*opening_quarter_usd as opening_changeorder_usd,
opening_changeorder*opening_quarter_rub as opening_changeorder_rub,
is_last_one,
Revenue*tl as revenue_tl,
Revenue*eur as revenue_eur,
Revenue*usd as revenue_usd,
Revenue*rub as revenue_rub,
lagged_revenue*lagged_tl as lagged_revenue_tl,
lagged_revenue*lagged_eur as lagged_revenue_eur,
lagged_revenue*lagged_usd as lagged_revenue_usd,
lagged_revenue*lagged_rub as lagged_revenue_rub,
opening_revenue*opening_quarter_tl as opening_revenue_tl,
opening_revenue*opening_quarter_eur as opening_revenue_eur,
opening_revenue*opening_quarter_usd as opening_revenue_usd,
opening_revenue*opening_quarter_rub as opening_revenue_rub,

RealizedCashIn*tl as cash_in_tl,
RealizedCashIn*eur as cash_in_eur,
RealizedCashIn*usd as cash_in_usd,
RealizedCashIn*rub as cash_in_rub,
lagged_realizedcashin*lagged_tl as lagged_cashin_tl,
lagged_realizedcashin*lagged_eur as lagged_cashin_eur,
lagged_realizedcashin*lagged_usd as lagged_cashin_usd,
lagged_realizedcashin*lagged_rub as lagged_cashin_rub,
opening_cashin*opening_quarter_tl as opening_cashin_tl,
opening_cashin*opening_quarter_eur as opening_cashin_eur,
opening_cashin*opening_quarter_usd as opening_cashin_usd,
opening_cashin*opening_quarter_rub as opening_cashin_rub,

RealizedCashOut*tl  as cash_out_tl,
RealizedCashOut*eur as cash_out_eur,
RealizedCashOut*usd as cash_out_usd,
RealizedCashOut*rub as cash_out_rub,
lagged_realizedcashout*lagged_tl as lagged_cashout_tl,
lagged_realizedcashout*lagged_eur as lagged_cashout_eur,
lagged_realizedcashout*lagged_usd as lagged_cashout_usd,
lagged_realizedcashout*lagged_rub as lagged_cashout_rub,
opening_cashout*opening_quarter_tl as opening_cashout_tl,
opening_cashout*opening_quarter_eur as opening_cashout_eur,
opening_cashout*opening_quarter_usd as opening_cashout_usd,
opening_cashout*opening_quarter_rub as opening_cashout_rub,

BudgetClaims*tl  as budgetclaim_tl,
BudgetClaims*eur as budgetclaim_eur,
BudgetClaims*usd as budgetclaim_usd,
BudgetClaims*rub as budgetclaim_rub,
lagged_budgetclaims*lagged_tl as lagged_budgetclaim_tl,
lagged_budgetclaims*lagged_eur as lagged_budgetclaim_eur,
lagged_budgetclaims*lagged_usd as lagged_budgetclaim_usd,
lagged_budgetclaims*lagged_rub as lagged_budgetclaim_rub,
opening_budget_claims*opening_quarter_tl as opening_budgetclaims_tl,
opening_budget_claims*opening_quarter_eur as opening_budgetclaims_eur,
opening_budget_claims*opening_quarter_usd as opening_budgetclaims_usd,
opening_budget_claims*opening_quarter_rub as opening_budgetclaims_rub,


spi,
lagged_spi,
spi_minus_lagged_spi,
spi_over_laggedspi,
spi_minus_opening_spi,
spi_over_opening_spi,

cpi,
lagged_cpi,
cpi_minus_lagged_cpi,
cpi_over_laggedcpi,
cpi_minus_openingcpi,
cpi_over_openingcpi,
project_code

FROM x
)

,final_cte_ind as (
SELECT

--changeorder_tl-revenue_tl as changeorder_minus_revenue_tl,
--changeorder_eur-revenue_eur as changeorder_minus_revenue_eur,
--changeorder_usd-revenue_usd as changeorder_minus_revenue_usd,
--changeorder_rub-revenue_rub as changeorder_minus_revenue_rub,
 [ObjectID] as object_id
,[RiskUnitID] as risk_unit_id
,[TemplateProcessID] as template_process_id
,[Code] as code
,[Year] as year
,[ProjectID] as project_id
,[YearQuarterID] as year_quarter_id
,[RequestUserID] as request_user_id
,[PrincibleType] as principle_type
,[CutOffDate] as cutoff_date
,[ContractCompletionDate] as contract_completion_date
,[PlannedCompletionDate] as planned_completion_date
,project_code as project_code
,year_quarter

,changeorder_tl,
changeorder_eur,
changeorder_usd,
changeorder_rub,

changeorder_tl/ NULLIF(revenue_tl ,0) as changeorder_over_revenue_tl,
changeorder_eur/NULLIF(revenue_eur,0) as changeorder_over_revenue_eur,
changeorder_usd/NULLIF(revenue_usd,0) as changeorder_over_revenue_usd,
changeorder_rub/NULLIF(revenue_rub,0) as changeorder_over_revenue_rub,

(changeorder_tl /NULLIF(revenue_tl ,0)) - (lagged_change_order_tl/ NULLIF(lagged_revenue_tl ,0)) changeorder_minus_revenue_previous_difference_tl,
(changeorder_eur/NULLIF(revenue_eur,0)) - (lagged_change_order_eur/NULLIF(lagged_revenue_eur,0)) changeorder_minus_revenue_previous_difference_eur,
(changeorder_usd/NULLIF(revenue_usd,0)) - (lagged_change_order_usd/NULLIF(lagged_revenue_usd,0)) changeorder_minus_revenue_previous_difference_usd,
(changeorder_rub/NULLIF(revenue_rub,0)) - (lagged_change_order_rub/NULLIF(lagged_revenue_rub,0)) changeorder_minus_revenue_previous_difference_rub,

((changeorder_tl/ NULLIF(revenue_tl ,0)) - (lagged_change_order_tl/ NULLIF(lagged_revenue_tl ,0)))/(NULLIF(lagged_change_order_tl ,0)/ NULLIF(lagged_revenue_tl ,0)) changeorder_over_revenue_previous_difference_tl,
((changeorder_eur/NULLIF(revenue_eur,0)) - (lagged_change_order_eur/NULLIF(lagged_revenue_eur,0)))/(NULLIF(lagged_change_order_eur,0) /NULLIF(lagged_revenue_eur,0)) changeorder_over_revenue_previous_difference_eur,
((changeorder_usd/NULLIF(revenue_usd,0)) - (lagged_change_order_usd/NULLIF(lagged_revenue_usd,0)))/(NULLIF(lagged_change_order_usd,0) /NULLIF(lagged_revenue_usd,0)) changeorder_over_revenue_previous_difference_usd,
((changeorder_rub/NULLIF(revenue_rub,0)) - (lagged_change_order_rub/NULLIF(lagged_revenue_rub,0)))/(NULLIF(lagged_change_order_rub,0) /NULLIF(lagged_revenue_rub,0)) changeorder_over_revenue_previous_difference_rub,

(changeorder_tl/ NULLIF(revenue_tl ,0)) - (opening_changeorder_tl/ NULLIF(opening_revenue_tl ,0)) changeorder_minus_revenue_opening_difference_tl,
(changeorder_eur/NULLIF(revenue_eur,0)) - (opening_changeorder_eur/NULLIF(opening_revenue_eur,0)) changeorder_minus_revenue_opening_difference_eur,
(changeorder_usd/NULLIF(revenue_usd,0)) - (opening_changeorder_usd/NULLIF(opening_revenue_usd,0)) changeorder_minus_revenue_opening_difference_usd,
(changeorder_rub/NULLIF(revenue_rub,0)) - (opening_changeorder_rub/NULLIF(opening_revenue_rub,0)) changeorder_minus_revenue_opening_difference_rub,

((changeorder_tl/NULLIF(revenue_tl,0)) - (opening_changeorder_tl/NULLIF(opening_revenue_tl,0)))/NULLIF((opening_changeorder_tl/NULLIF(opening_revenue_tl,0)),0) changeorder_over_revenue_opening_difference_tl,
((changeorder_eur/NULLIF(revenue_eur,0)) - (opening_changeorder_eur/NULLIF(opening_revenue_eur,0)))/NULLIF((opening_changeorder_eur/NULLIF(opening_revenue_eur,0)),0) changeorder_over_revenue_opening_difference_eur,
((changeorder_usd/NULLIF(revenue_usd,0)) - (opening_changeorder_usd/NULLIF(opening_revenue_usd,0)))/NULLIF((opening_changeorder_usd/NULLIF(opening_revenue_usd,0)),0) changeorder_over_revenue_opening_difference_usd,
((changeorder_rub/NULLIF(revenue_rub,0)) - (opening_changeorder_rub/NULLIF(opening_revenue_rub,0)))/NULLIF((opening_changeorder_rub/NULLIF(opening_revenue_rub,0)),0) changeorder_over_revenue_opening_difference_rub,

budgetclaim_tl,
budgetclaim_eur,
budgetclaim_usd,
budgetclaim_rub,

revenue_tl,
revenue_eur,
revenue_usd,
revenue_rub,

budgetclaim_tl/ NULLIF(revenue_tl ,0)  as budgetclaim_over_revenue_tl,
budgetclaim_eur/NULLIF(revenue_eur,0) as budgetclaim_over_revenue_eur,
budgetclaim_usd/NULLIF(revenue_usd,0) as budgetclaim_over_revenue_usd,
budgetclaim_rub/NULLIF(revenue_rub,0) as  budgetclaim_over_revenue_rub,
			
budgetclaim_tl/ NULLIF(revenue_tl ,0)- lagged_budgetclaim_tl/NULLIF(lagged_revenue_tl,0) as budgetclaim_minus_revenue_tl,
budgetclaim_eur/NULLIF(revenue_eur,0) - lagged_budgetclaim_eur/NULLIF(lagged_revenue_eur,0)  as budgetclaim_minus_revenue_eur,
budgetclaim_usd/NULLIF(revenue_usd,0) - lagged_budgetclaim_usd/NULLIF(lagged_revenue_usd,0)  as budgetclaim_minus_revenue_usd,
budgetclaim_rub/NULLIF(revenue_rub,0) - lagged_budgetclaim_rub/NULLIF(lagged_revenue_rub,0)  as budgetclaim_minus_revenue_rub,

(budgetclaim_tl/ NULLIF(revenue_tl  ,0)  - lagged_budgetclaim_tl/   NULLIF(lagged_revenue_tl,0) )/  NULLIF(  (lagged_budgetclaim_tl/NULLIF(lagged_revenue_tl ,0)),0) as laggedbudgetclaim_over_revenue_tl,
(budgetclaim_eur/NULLIF(revenue_eur,0) - lagged_budgetclaim_eur/NULLIF(lagged_revenue_eur,0))/NULLIF((lagged_budgetclaim_eur/NULLIF(lagged_revenue_eur,0)),0)  as laggedbudgetclaim_over_revenue_eur,
(budgetclaim_usd/NULLIF(revenue_usd,0) - lagged_budgetclaim_usd/NULLIF(lagged_revenue_usd,0))/NULLIF((lagged_budgetclaim_usd/NULLIF(lagged_revenue_usd,0)),0)  as laggedbudgetclaim_over_revenue_usd,
(budgetclaim_rub/NULLIF(revenue_rub,0) - lagged_budgetclaim_rub/NULLIF(lagged_revenue_rub,0))/NULLIF((lagged_budgetclaim_rub/NULLIF(lagged_revenue_rub,0)),0)  as laggedbudgetclaim_over_revenue_rub,

budgetclaim_tl/ NULLIF(revenue_tl ,0)  - opening_budgetclaims_tl/ NULLIF(opening_revenue_tl  ,0)   as budgetclaim_minus_openingrevenue_tl,
budgetclaim_eur/NULLIF(revenue_eur,0) - opening_budgetclaims_eur/NULLIF(opening_revenue_eur ,0) as budgetclaim_minus_openingrevenue_eur,
budgetclaim_usd/NULLIF(revenue_usd,0) - opening_budgetclaims_usd/NULLIF(opening_revenue_usd ,0) as budgetclaim_minus_openingrevenue_usd,
budgetclaim_rub/NULLIF(revenue_rub,0) - opening_budgetclaims_rub/NULLIF(opening_revenue_rub ,0) as budgetclaim_minus_openingrevenue_rub,

(budgetclaim_tl/ NULLIF(revenue_tl  ,0)  - opening_budgetclaims_tl/NULLIF(opening_revenue_tl,0))/  NULLIF((opening_budgetclaims_tl/   NULLIF(opening_revenue_tl ,0)),0)   as budgetclaim_over_openingrevenue_tl,
(budgetclaim_eur/NULLIF(revenue_eur ,0)- opening_budgetclaims_eur/NULLIF( opening_revenue_eur,0))/NULLIF((opening_budgetclaims_eur/NULLIF(opening_revenue_eur,0))  ,0) as budgetclaim_over_openingrevenue_eur,
(budgetclaim_usd/NULLIF(revenue_usd ,0)- opening_budgetclaims_usd/NULLIF(opening_revenue_usd,0))/NULLIF((opening_budgetclaims_usd/NULLIF(opening_revenue_usd,0))  ,0) as budgetclaim_over_openingrevenue_usd,
(budgetclaim_rub/NULLIF(revenue_rub ,0)- opening_budgetclaims_rub/NULLIF(opening_revenue_rub,0))/NULLIF((opening_budgetclaims_rub/NULLIF(opening_revenue_rub,0))  ,0) as budgetclaim_over_openingrevenue_rub,

cash_in_tl,
cash_in_eur,
cash_in_usd,
cash_in_rub,

cash_out_tl,
cash_out_eur,
cash_out_usd,
cash_out_rub,

cash_in_tl-cash_out_tl  as net_cash_tl,
cash_in_eur-cash_out_eur as net_cash_eur,
cash_in_usd-cash_out_usd as net_cash_usd,
cash_in_rub-cash_out_rub as net_cash_rub,

(cash_in_tl-cash_out_tl)-   (lagged_cashin_tl-lagged_cashout_tl) as netcash_minus_netcashlagged_tl,
(cash_in_eur-cash_out_eur)- (lagged_cashin_eur-lagged_cashout_eur) as netcash_minus_netcashlagged_eur,
(cash_in_usd-cash_out_usd)- (lagged_cashin_usd-lagged_cashout_usd) as netcash_minus_netcashlagged_usd,
(cash_in_rub-cash_out_rub)- (lagged_cashin_rub-lagged_cashout_rub) as netcash_minus_netcashlagged_rub,

((cash_in_tl-cash_out_tl) -  (lagged_cashin_tl-lagged_cashout_tl))/  NULLIF((lagged_cashin_tl-lagged_cashout_tl)  ,0) as netcash_over_netcashlagged_tl,
((cash_in_eur-cash_out_eur)- (lagged_cashin_eur-lagged_cashout_eur))/NULLIF((lagged_cashin_eur-lagged_cashout_eur),0) as netcash_over_netcashlagged_eur,
((cash_in_usd-cash_out_usd)- (lagged_cashin_usd-lagged_cashout_usd))/NULLIF((lagged_cashin_usd-lagged_cashout_usd),0) as netcash_over_netcashlagged_usd,
((cash_in_rub-cash_out_rub)- (lagged_cashin_rub-lagged_cashout_rub))/NULLIF((lagged_cashin_rub-lagged_cashout_rub),0) as netcash_over_netcashlagged_rub,

((cash_in_tl-cash_out_tl) -  (opening_cashin_tl-opening_cashout_tl))  as netcash_minus_opening_tl,
((cash_in_eur-cash_out_eur)- (opening_cashin_eur-opening_cashin_eur)) as netcash_minus_opening_eur,
((cash_in_usd-cash_out_usd)- (opening_cashin_usd-opening_cashin_usd)) as netcash_minus_opening_usd,
((cash_in_rub-cash_out_rub)- (opening_cashin_rub-opening_cashin_rub)) as netcash_minus_opening_rub,

((cash_in_tl-cash_out_tl) -  (opening_cashin_tl-opening_cashout_tl)) /NULLIF((opening_cashin_tl - opening_cashout_tl), 0) as netcash_over_opening_tl,
((cash_in_eur-cash_out_eur)- (opening_cashin_eur-opening_cashin_eur))/NULLIF((opening_cashin_eur-opening_cashout_eur), 0) as netcash_over_opening_eur,
((cash_in_usd-cash_out_usd)- (opening_cashin_usd-opening_cashin_usd))/NULLIF((opening_cashin_usd-opening_cashout_usd), 0) as netcash_over_opening_usd,
((cash_in_rub-cash_out_rub)- (opening_cashin_rub-opening_cashin_rub))/NULLIF((opening_cashin_rub-opening_cashout_rub), 0) as netcash_over_opening_rub,

spi,
lagged_spi,
spi_minus_lagged_spi,
spi_over_laggedspi,
spi_minus_opening_spi,
spi_over_opening_spi,

cpi,
lagged_cpi,
cpi_minus_lagged_cpi,
cpi_over_laggedcpi,
cpi_minus_openingcpi,
cpi_over_openingcpi,
is_last_one
FROM final_cte

)
SELECT 
[object_id]
      ,[risk_unit_id]
      ,[template_process_id]
      ,[code]
      ,[year]
      ,[project_id]
      ,[year_quarter_id]
      ,[request_user_id]
      ,[principle_type]
      ,[cutoff_date]
      ,[contract_completion_date]
      ,[planned_completion_date]
      ,[project_code]
      ,[year_quarter]
      ,[changeorder_tl]
      ,[changeorder_eur]
      ,[changeorder_usd]
      ,[changeorder_rub]
      ,[changeorder_over_revenue_tl]								
      ,[changeorder_over_revenue_eur]
      ,[changeorder_over_revenue_usd]
      ,[changeorder_over_revenue_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_minus_revenue_previous_difference_tl]	END AS [changeorder_minus_revenue_previous_difference_tl]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_minus_revenue_previous_difference_eur]END AS [changeorder_minus_revenue_previous_difference_eur]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_minus_revenue_previous_difference_usd]END AS [changeorder_minus_revenue_previous_difference_usd]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_minus_revenue_previous_difference_rub]END AS [changeorder_minus_revenue_previous_difference_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_over_revenue_previous_difference_tl]	END AS [changeorder_over_revenue_previous_difference_tl]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_over_revenue_previous_difference_eur]	END AS [changeorder_over_revenue_previous_difference_eur]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_over_revenue_previous_difference_usd]	END AS [changeorder_over_revenue_previous_difference_usd]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_over_revenue_previous_difference_rub]	END AS [changeorder_over_revenue_previous_difference_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_minus_revenue_opening_difference_tl]	END AS [changeorder_minus_revenue_opening_difference_tl]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_minus_revenue_opening_difference_eur]	END AS [changeorder_minus_revenue_opening_difference_eur]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_minus_revenue_opening_difference_usd]	END AS [changeorder_minus_revenue_opening_difference_usd]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_minus_revenue_opening_difference_rub]	END AS [changeorder_minus_revenue_opening_difference_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_over_revenue_opening_difference_tl]	END AS [changeorder_over_revenue_opening_difference_tl]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_over_revenue_opening_difference_eur]	END AS [changeorder_over_revenue_opening_difference_eur]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_over_revenue_opening_difference_usd]	END AS [changeorder_over_revenue_opening_difference_usd]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [changeorder_over_revenue_opening_difference_rub]	END AS [changeorder_over_revenue_opening_difference_rub]
      ,[budgetclaim_tl]																							
      ,[budgetclaim_eur]																						
      ,[budgetclaim_usd]																						
      ,[budgetclaim_rub]																						
      ,[revenue_tl]																								
      ,[revenue_eur]																							
      ,[revenue_usd]																							
      ,[revenue_rub]																							
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_over_revenue_tl]						END AS [budgetclaim_over_revenue_tl]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_over_revenue_eur]						END AS [budgetclaim_over_revenue_eur]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_over_revenue_usd]						END AS [budgetclaim_over_revenue_usd]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_over_revenue_rub]						END AS [budgetclaim_over_revenue_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_minus_revenue_tl]						END AS [budgetclaim_minus_revenue_tl]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_minus_revenue_eur]					END AS [budgetclaim_minus_revenue_eur]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_minus_revenue_usd]					END AS [budgetclaim_minus_revenue_usd]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_minus_revenue_rub]					END AS [budgetclaim_minus_revenue_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [laggedbudgetclaim_over_revenue_tl]				END AS [laggedbudgetclaim_over_revenue_tl]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [laggedbudgetclaim_over_revenue_eur]				END AS [laggedbudgetclaim_over_revenue_eur]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [laggedbudgetclaim_over_revenue_usd]				END AS [laggedbudgetclaim_over_revenue_usd]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [laggedbudgetclaim_over_revenue_rub]				END AS [laggedbudgetclaim_over_revenue_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_minus_openingrevenue_tl]				END AS [budgetclaim_minus_openingrevenue_tl]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_minus_openingrevenue_eur]				END AS [budgetclaim_minus_openingrevenue_eur]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_minus_openingrevenue_usd]				END AS [budgetclaim_minus_openingrevenue_usd]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_minus_openingrevenue_rub]				END AS [budgetclaim_minus_openingrevenue_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_over_openingrevenue_tl]				END AS [budgetclaim_over_openingrevenue_tl]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_over_openingrevenue_eur]				END AS [budgetclaim_over_openingrevenue_eur]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_over_openingrevenue_usd]				END AS [budgetclaim_over_openingrevenue_usd]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [budgetclaim_over_openingrevenue_rub]				END AS [budgetclaim_over_openingrevenue_rub]
      ,[cash_in_tl]
      ,[cash_in_eur]
      ,[cash_in_usd]
      ,[cash_in_rub]
      ,[cash_out_tl]
      ,[cash_out_eur]
      ,[cash_out_usd]
      ,[cash_out_rub]
      ,[net_cash_tl]
      ,[net_cash_eur]
      ,[net_cash_usd]
      ,[net_cash_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_minus_netcashlagged_tl]				END AS 		   [netcash_minus_netcashlagged_tl]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_minus_netcashlagged_eur]				END AS 		   [netcash_minus_netcashlagged_eur]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_minus_netcashlagged_usd]				END AS 		   [netcash_minus_netcashlagged_usd]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_minus_netcashlagged_rub]				END AS 		   [netcash_minus_netcashlagged_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_over_netcashlagged_tl]				END AS 		   [netcash_over_netcashlagged_tl]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_over_netcashlagged_eur]				END AS 		   [netcash_over_netcashlagged_eur]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_over_netcashlagged_usd]				END AS 		   [netcash_over_netcashlagged_usd]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_over_netcashlagged_rub]				END AS 		   [netcash_over_netcashlagged_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_minus_opening_tl]						END AS 		   [netcash_minus_opening_tl]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_minus_opening_eur]					END AS 		   [netcash_minus_opening_eur]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_minus_opening_usd]					END AS 		   [netcash_minus_opening_usd]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_minus_opening_rub]					END AS 		   [netcash_minus_opening_rub]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_over_opening_tl]						END AS 		   [netcash_over_opening_tl]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_over_opening_eur]						END AS 		   [netcash_over_opening_eur]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_over_opening_usd]						END AS 		   [netcash_over_opening_usd]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [netcash_over_opening_rub]						END AS 		   [netcash_over_opening_rub]
      ,[spi]																						
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [lagged_spi]									END AS 		   [lagged_spi]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [spi_minus_lagged_spi]							END AS 		   [spi_minus_lagged_spi]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [spi_over_laggedspi]							END AS 		   [spi_over_laggedspi]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [spi_minus_opening_spi]						END AS 		   [spi_minus_opening_spi]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [spi_over_opening_spi]							END AS 		   [spi_over_opening_spi]
      ,[cpi]																							
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [lagged_cpi]									END AS 		   [lagged_cpi]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [cpi_minus_lagged_cpi]							END AS 		   [cpi_minus_lagged_cpi]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [cpi_over_laggedcpi]							END AS 		   [cpi_over_laggedcpi]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [cpi_minus_openingcpi]							END AS 		   [cpi_minus_openingcpi]
      ,case when year_quarter_id IN ('1','2') THEN NULL ELSE [cpi_over_openingcpi]							END AS 		   [cpi_over_openingcpi]
      ,[is_last_one]
FROM final_cte_ind