{{
  config(
    materialized = 'table',tags = ['supportops_kpi']
    )
}}	
WITH project_company_mapping AS (
	SELECT
		name1
		,WERKS
		,w.BWKEY
		,bukrs
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} w
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} k ON w.bwkey = k.bwkey
)

SELECT
	rls_region = COALESCE(cm.RegionCode,'NAR')
	,rls_group = CONCAT(cm.KyribaGrup, '_' , cm.RegionCode)
	,rls_company = CONCAT(cm.RobiKisaKod, '_', cm.RegionCode)
	,rls_businessarea = CONCAT(sp.business_area, '_', cm.RegionCode)
  ,[group] = cm.KyribaGrup
  ,[company] = cm.RobiKisaKod
  ,[source]
  ,[fiscal_year]
  ,[fiscal_month]
  ,[personnel_no]
  ,[personnel_field]
  ,[personnel_subfield]
  ,[business_area]
  ,[business_area_name]
  ,[subcontractor_no]
  ,[subcontractor_name]
  ,[national_id_number]
  ,[full_name]
  ,[username]
  ,[position_description]
  ,[team_no]
  ,[manager_id]
  ,CASE
    WHEN [manager_fullname] = '' THEN 'NO MANAGER'
    ELSE [manager_fullname]
  END AS [manager_fullname]
  ,[team_description]
  ,[work_location]
  ,[start_date]
  ,[end_date]
  ,[leaving_reason]
  ,[standard_work_hours]
  ,[weekly_rest_hours]
  ,[weekend_work_hours]
  ,[public_holiday_hours]
  ,[public_holiday_work_hours]
  ,[annual_leave_hours]
  ,[work_accident_hours]
  ,[paid_excused_leave_hours]
  ,[unpaid_excused_leave_hours]
  ,[absence_hours]
  ,[normal_payable_days]
  ,[overtime_hours]
  ,[holiday_work_hours]
  ,[overtime_status]
  ,[sgk_days_count]
  ,[payable_days_to_employee]
  ,[overtime_multiplier_hours]
  ,[holiday_work_multiplier_hours]
  ,[weekend_work_multiplier_hours]
  ,[final_sgk_day]
  ,[daily_wage]
  ,[hourly_wage]
  ,[salary_amount]
  ,[overtime_pay]
  ,[holiday_overtime_pay]
  ,[weekend_overtime_pay]
  ,[total_overtime_payments]
  ,[total_payable_to_employee]
  ,[payroll_1]
  ,[payroll_2]
  ,[hourly_overtime_company_difference]
  ,[company_difference]
  ,[daily_sgk_fee]
  ,[other_profit]
  ,[unit_price_of_progress_payment]
  ,[company_sgk_payment]
  ,[company_share]
  ,[company_overtime_payment]
  ,[severance_gross]
  ,[severance_net]
  ,[notice_gross]
  ,[notice_net]
  ,[annual_leave_employer_cost]
  ,[annual_leave_gross]
  ,[annual_leave_net]
  ,[total_payable_to_company]
  ,[other_expenses]
  ,[company_profit_share_payment]
  ,[empty_day]
  ,[profit_ration]
  ,[stamp_tax]
  ,[work_accident_medical_report_hours]
  ,[overtime_correction]
FROM {{ ref('stg_supportops_kpi_t_fact_supportlabourunion') }} sp
LEFT JOIN project_company_mapping pcm ON pcm.werks = sp.business_area
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm ON cm.RobiKisaKod = pcm.bukrs

