{{
  config(
    materialized = 'table',tags = ['risk_kpi']
    )
}}
WITH x as (
SELECT
r.Code as register_code,
ri.Code as register_item_code,
ri.description as register_item_description, --ri.description
ri.Type as risk_item_type,
case when ri.Status = '1' then 'Realized,Budgeted'
when ri.Status = '3' then 'Ongoing'
when ri.Status = '2' then 'Not Realized, Avoided'
 else cast(ri.Status as varchar(max)) end  as risk_item_status,
pr.Name as project_name,
pr.ObjectID as project_id,
FinancialImpact as financial_impact,
ProfitImpactPrice as profit_impact_price,
BestCasePrice as best_case_price,
BestCasePrice*c.Value as bestcaseprice_tl,
BestCasePrice*c1.Value as bestcaseprice_usd,
BestCasePrice*c2.Value as bestcaseprice_eur,
BestCasePrice*c3.Value as bestcaseprice_rub,
BestCasePercent as best_case_percent,
RealCasePrice as real_case_price,
RealCasePrice*c.Value as realcaseprice_tl,
RealCasePrice*c1.Value as realcaseprice_usd,
RealCasePrice*c2.Value as realcaseprice_eur,
RealCasePrice*c3.Value as realcaseprice_rub,
RealCasePercent as real_case_percent,
WorstCasePrice as worst_case_price,
WorstCasePrice*c.Value as worstcaseprice_tl,
WorstCasePrice*c1.Value as worstcaseprice_usd,
WorstCasePrice*c2.Value as worstcaseprice_eur,
WorstCasePrice*c3.Value as worstcaseprice_rub,
WorstCasePercent as worst_case_percent,
ResponsibleUserID as responsible_user_id,
su.FullName as full_name,
su.Title as title,
su.GlobalID as global_id,
case when ResponseType = '1' then 'Accept'
     when ResponseType = '2' then 'Mitigate' end as response_type,
Deadline as deadline,
RiskOfficeNote as risk_office_note,
StrategyNote as strategy_note, 
CalculateAnalysisNote as calculate_analyisis_note,
CalculateNote as calculate_note,
Note as note,
RiskTypeID as risk_type_id,
co.NAME as company_name,
case
	when YearQuarterID = '1' THEN CONCAT('Tender', r.Year )
	when YearQuarterID = '2' THEN CONCAT('Opening',  r.Year )
	when YearQuarterID = '3' THEN CONCAT(r.Year,'-','Q1' )
	when YearQuarterID = '4' THEN CONCAT(r.Year,'-','Q2')
	when YearQuarterID = '5' THEN CONCAT(r.Year,'-','Q3')
	when YearQuarterID = '6' THEN CONCAT(r.Year,'-','Q4')
ELSE NULL END AS  year_quarter,
r.Year as year,
drf.Code as risk_type_code,
case 
	when r.StatusID = '1'  THEN 'Draft'
	when r.StatusID = '2'  THEN 'InApprovalProgress'
	when r.StatusID = '3'  THEN 'Completed'
	when r.StatusID = '4'  THEN 'Cancelled'
end as risk_register_status_id,
r.CreatedDate,
pr.Code as project_code
,r.Revenue as revenue
,r.ExpenseBudget as expense_budget
,r.Profit as profit
,r.ProfitPercent as profit_percent
,case when ri.Type = '1' then 'Risk'
      when ri.Type = '2' then 'Opportunity'
end as risk_or_opportunity
,m.last_time_profit
,m.last_quarter
,rs.code as risk_cause_code
,rs.Name as risk_cause_name
,rcs.Code as risk_cause_sub_code
,rcs.Name as risk_cause_sub_name
,CONCAT(rs.code,'.',rcs.Code,'-',rcs.Name) as top_types
,CONCAT(rs.code,'.',rcs.Code,'-',rs.Name,'-',rcs.Name,'-',drt.Name) as top_types_table
,drt.Name as risk_type_name
,ri.Impact as risk_item_impact
,ri.Probability as risk_item_probability
,ri.Rating as risk_item_rating
,ri.RiskGroup as risk_item_group
,ri.IsRiskTop as is_top_risk
,DENSE_RANK() OVER (
    PARTITION BY pr.Name
    ORDER BY Year DESC, 
            YearQuarterID desc
) AS QuarterRankDESC
,DENSE_RANK() OVER (
    PARTITION BY pr.Name
    ORDER BY Year ASC, 
            YearQuarterID ASC
) AS QuarterRankASC,
YearQuarterID as year_quarter_id,
CASE
	WHEN DENSE_RANK() OVER (
    PARTITION BY pr.Name
    ORDER BY Year ASC, 
            YearQuarterID ASC
)<=2 THEN '1' 
	WHEN DENSE_RANK() OVER (
    PARTITION BY pr.Name
    ORDER BY Year DESC, 
            YearQuarterID desc
	)<=2 THEN '1' 
	else null
	end as  is_firsttwo_or_lasttwo,
case  
	WHEN DENSE_RANK() OVER (
    PARTITION BY pr.Name
    ORDER BY Year DESC, 
            YearQuarterID desc
	)<=1 THEN '1' 
	else null
	end as  is_last_one,
     c.Value as tl,
     c1.Value as usd,
     c2.Value as eur,
     c3.Value as rub
FROM PRODAPPSDB.RNS_RISK_PROD.[dbo].[RGR_REGISTERS] r
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[RGR_REGISTER_ITEMS] ri on ri.RegisterID = r.ObjectID 	and ri.IsActive = '1' and ri.IsDeleted = '0' 	  and ri.IsActive = '1'  and ri.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].PRJ_PROJECTS pr on pr.ObjectID = r.ProjectID 	  and pr.IsActive = '1' and pr.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].DEF_COMPANIES co on co.ObjectID = r.CompanyID 	  and co.IsActive = '1'  and co.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].DEF_RISK_TYPES drf on drf.ObjectID = ri.RiskTypeID 	  and drf.IsActive = '1' and drf.IsDeleted = '0'

	LEFT JOIN 
				(SELECT
				ROW_NUMBER() OVER(PARTITION BY r.ProjectID ORDER BY r.YearQuarterID DESC) as rn_1,
				 r.YearQuarterID as last_quarter,
				r.ProjectID as project,
				Profit as last_time_profit
				FROM PRODAPPSDB.RNS_RISK_PROD.[dbo].RGR_REGISTERS r)

				m on m.project = r.ProjectID and rn_1 = '1'
	LEFT JOIN  PRODAPPSDB.RNS_RISK_PROD.[dbo].V_DEF_RISK_CAUSES rs on rs.ObjectID =ri.RiskCauseID and rs.LangCode = 'en' 	  and rs.IsDeleted = '0' and rs.IsActive = '1'
	LEFT JOIN  PRODAPPSDB.RNS_RISK_PROD.[dbo].V_DEF_RISK_CAUSE_SUBS rcs on ri.RiskCauseSubID = rcs.ObjectID  and rcs.LangCode = 'en' 	  and rcs.IsDeleted = '0' and rcs.IsActive = '1'
	LEFT JOIN  PRODAPPSDB.RNS_RISK_PROD.[dbo].V_DEF_RISK_TYPES  drt on drt.ObjectID = ri.RiskTypeID and drt.LangCode = 'en' AND drt.IsDeleted = '0' and drt.IsActive = '1'

	      left join PRODAPPSDB.RNS_RISK_PROD.[dbo].RGR_REGISTER_CURRENCIES c on c.RegisterID = r.ObjectID and c.DisplayName = 'TL' and c.IsActive  = '1' and c.IsDeleted = '0'
        left join PRODAPPSDB.RNS_RISK_PROD.[dbo].RGR_REGISTER_CURRENCIES c1 on c1.RegisterID = r.ObjectID and c1.DisplayName = 'USD' and c1.IsActive  = '1' and c1.IsDeleted = '0'
        left join PRODAPPSDB.RNS_RISK_PROD.[dbo].RGR_REGISTER_CURRENCIES c2 on c2.RegisterID = r.ObjectID and c2.DisplayName = 'EUR' and c2.IsActive  = '1' and c2.IsDeleted = '0'
        left join PRODAPPSDB.RNS_RISK_PROD.[dbo].RGR_REGISTER_CURRENCIES c3 on c3.RegisterID = r.ObjectID and c3.DisplayName = 'RUB' and c3.IsActive  = '1' and c3.IsDeleted = '0'
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[SYS_USERS] su ON su.ObjectID = ri.ResponsibleUserID and su.IsActive = '1' and su.IsDeleted ='0' and su.[LanguageID] = '1'
where 1=1
	  and r.IsActive = '1'
	  and r.IsDeleted = '0'
	  and r.StatusID = '3'
)

SELECT
x.*,
case when is_last_one = '1' then bestcaseprice_tl else null end as bestcaseprice_last_tl,
case when is_last_one = '1' then bestcaseprice_eur else null end as bestcaseprice_last_eur,
case when is_last_one = '1' then bestcaseprice_usd else null end as bestcaseprice_last_usd,
case when is_last_one = '1' then bestcaseprice_rub else null end as bestcaseprice_last_rub,

case when is_last_one = '1' then realcaseprice_tl else null end as realcaseprice_last_tl,
case when is_last_one = '1' then realcaseprice_eur else null end as realcaseprice_last_eur,
case when is_last_one = '1' then realcaseprice_usd else null end as realcaseprice_last_usd,
case when is_last_one = '1' then realcaseprice_rub else null end as realcaseprice_last_rub,

case when is_last_one = '1' then worstcaseprice_tl else null end as worstcaseprice_last_tl,
case when is_last_one = '1' then worstcaseprice_eur else null end as worstcaseprice_last_eur,
case when is_last_one = '1' then worstcaseprice_usd else null end as worstcaseprice_last_usd,
case when is_last_one = '1' then worstcaseprice_rub else null end as worstcaseprice_last_rub,

case when is_last_one = '1' then profit*tl else null end as profit_last_tl,
case when is_last_one = '1' then profit*eur else null end as profit_last_eur,
case when is_last_one = '1' then profit*usd else null end as profit_last_usd,
case when is_last_one = '1' then profit*rub else null end as profit_last_rub,

financial_impact*tl as financial_impact_tl,
financial_impact*eur as financial_impact_eur,
financial_impact*usd as financial_impact_usd,
financial_impact*rub as financial_impact_rub,

profit_impact_price*tl as profit_impact_tl,
profit_impact_price*eur as profit_impact_eur,
profit_impact_price*usd as profit_impact_usd,
profit_impact_price*rub  as profit_impact_rub

FROM x