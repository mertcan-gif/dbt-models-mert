{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
WITH IlerlemeOzet AS (
	SELECT * FROM (	
		SELECT 
			project_id
			,reporting_date
			,db_upload_timestamp
			,DENSE_RANK() OVER(PARTITION BY project_id, reporting_date ORDER BY db_upload_timestamp DESC) AS update_rank
			,SUM(total_man_hour) AS total_man_hour
			,SUM(earned_man_hour) AS earned_man_hour
			,SUM(actual_man_hour) AS actual_man_hour
			,SUM(planned_man_hour) AS planned_man_hour
		FROM {{source('stg_to_kpi','raw__to_kpi_t_fact_progressdetails' )}}
		GROUP BY
			project_id
			,reporting_date
			,db_upload_timestamp
		) IlerlemeDetay_Raw
	WHERE 1=1
		AND update_rank = 1
),

PersonelOzet AS (

	SELECT 
	project_id
	,reporting_date
	,total_personnel = SUM([value])
	,renaissance_personnel = SUM(CASE WHEN reporting_group = N'Rönesans' THEN [value] END)
	,subcontractor_personnel =  SUM(CASE WHEN reporting_group = N'Subcontractor' THEN [value] END)
	,renaissance_direct_personnel =  SUM(CASE WHEN reporting_sub_group = N'Rönesans' AND type = N'direct' THEN [value] END)
	,renaissance_indirect_personnel =  SUM(CASE WHEN reporting_sub_group = N'Rönesans' AND type = N'indirect' THEN [value] END)
	,renaissance_support_personnel =  SUM(CASE WHEN reporting_sub_group = N'Rönesans' AND type = N'support' THEN [value] END)
	,subcontractor_direct_personnel =  SUM(CASE WHEN reporting_group = N'Subcontractor' AND reporting_sub_group <> N'Subcontractor' THEN [value] END)
	,subcontractor_indirect_personnel = SUM(CASE WHEN reporting_sub_group = N'Subcontractor' AND type = N'indirect' THEN [value] END)
	,subcontractor_support_personnel =  SUM(CASE WHEN reporting_sub_group = N'Subcontractor' AND type = N'support' THEN [value] END)
	,personnel_civil = SUM(CASE WHEN reporting_sub_group LIKE N'%Civil%' THEN [value] END)
	,personnel_mechanical = SUM(CASE WHEN reporting_sub_group = N'Mechanical' THEN [value] END)
	,personnel_electrical = SUM(CASE WHEN reporting_sub_group = N'Electrical' THEN [value] END)
	FROM (
		SELECT
			*
			,DENSE_RANK() OVER(PARTITION BY project_id, reporting_date ORDER BY db_upload_timestamp DESC) AS update_rank
		FROM {{source('stg_to_kpi','raw__to_kpi_t_fact_personneldetails')}}
		) PersonelOzet_Raw
	WHERE 1=1
		AND update_rank = 1
	GROUP BY project_id
	,reporting_date



),

All_View_Raw AS (

	SELECT
		[rls_region] = Prj.[region]
		,[rls_group]=Prj.[group]								
		,[rls_company] = Prj.[company]						
		,[rls_businessarea] = Prj.[business_area]	
		,Prj.[group]											
		,Prj.[company]	
		,Prj.[business_area]									
		,Prj.[contractor]										
		,Prj.[status]											
		,Prj.[country]										
		,Prj.[city]											
		,Prj.[currency]										
		,Prj.[project_name]									
		,Prj.[project_shortname]								
		,[location]	= Prj.[city]								
		,All_t.project_id										
		,All_t.data_entry_timestamp								
		,All_t.reporting_date									
		,All_t.employer											
		,All_t.contract_amount									
		,Prj.contract_type									
		,All_t.project_commencement_date						
		,All_t.project_finish_date								
		,All_t.gba												
		,budget_revision_period	= NULL -- Trashtest'te bastığımız tabloda bu kolon yok, aws'de bulunuyor fakat NULL.						
		,All_t.baseline_revision_date							
		,All_t.completion_accrual_contract_amount				
		,All_t.invoiced_progress_payment_amount_ipd_evat		
		,All_t.invoice_pending_progress_payment					
		,All_t.interim_payment_progress_epd						
		,All_t.interim_payment_progress_based_on_contract		
		,change_order = All_t.progress_payment_change_order 	
		,All_t.cash_recieved									
		,All_t.at_completion_cash								
		,All_t.overdue_receivables								
		,All_t.total_recievables								
		,All_t.total_advance_payment_amount						
		,All_t.remaining_advance_payment_amount					
		,All_t.advance_payment_guarantee_letter_amount			
		,All_t.advance_payment_guarantee_letter_end_date		
		,All_t.performance_guarantee_letter_amount				
		,All_t.performance_guarantee_letter_end_date			
		,All_t.site_delivery_date								
		,All_t.contract_finish_date								
		,All_t.target_finish_date
		,All_t.contract_start_date
		,All_t.project_duration
		,All_t.db_upload_timestamp	
		,progress_incl_price_difference_and_uninvoiced_progress_payment = NULL
		,progress_incl_co_and_uninvoiced_progress_payment = NULL
		,progress_incl_all = NULL

		,physical_progress = I_o.earned_man_hour / I_o.total_man_hour
		,planned_progress = I_o.planned_man_hour / I_o.total_man_hour
		,All_t.time_progress

		,realized_duration = -- Formül: Raporlama Tarihi ile Yer Teslim Tarihi arasında geçen süre (gün)
			DATEDIFF(DAY, All_t.site_delivery_date,All_t.reporting_date)	

		,remaining_duration = -- Formül: Proje Süresi ile Gerçekleşen Süre arasında geçen süre (gün)
			All_t.project_duration
			- DATEDIFF(DAY, All_t.site_delivery_date,All_t.reporting_date)	

		,targeted_remaining_duration = -- Formül: Yer Teslim Tarihi ile Hedeflenen Bitiş Tarihi arasındaki gün sayısı ile Gerçekleşen Süre arasındaki fark
			DATEDIFF(DAY, All_t.site_delivery_date, All_t.target_finish_date)
			- DATEDIFF(DAY, All_t.site_delivery_date,All_t.reporting_date)	

		,contractual_remaining_duration = -- Formül: Yer Teslim Tarihi ile Sözleşme Bitiş Tarihi arasındaki gün sayısı ile Gerçekleşen Süre arasındaki fark
			DATEDIFF(DAY, All_t.site_delivery_date, All_t.contract_finish_date)
			- DATEDIFF(DAY, All_t.site_delivery_date,All_t.reporting_date)	
	
		,interim_payment_progress = -- Formül: Faturalı Başberi Hakediş Tutari (FF Dahil, KDV Hariç) / İş Sonu Tahakkuk Sözleşme Tutarı (FF Dahil, KDV Hariç)
			ISNULL(All_t.invoiced_progress_payment_amount_ipd_evat,0) 
			/ ISNULL(All_t.completion_accrual_contract_amount,0)
	
		,cash_progress = -- Formül: Gerçekleşen Tahsilat (KDV, CO ve FF Dahil) / İş Sonu Tahsilat Bedeli (KDV, CO ve FF Dahil)
			ISNULL(All_t.cash_recieved,0) 
			/ ISNULL(All_t.at_completion_cash,0)
	
		,progress_payment_contract = -- Formül: Sözleşme Bedeli * Hakediş İlerlemesi (Sözleşme Üzerinden)
			ISNULL(All_t.contract_amount,0) 
			* ISNULL(All_t.interim_payment_progress_based_on_contract,0)
	
		,price_difference_at_completion = -- Formül: İş Sonu Tahakkuk Sözleşme Tutarı (FF Dahil, KDV Hariç) - İlave İş (İş Sonu) - Sözleşme Bedeli
			CASE	
				WHEN All_t.interim_payment_progress_epd = 0 THEN NULL
				ELSE 
					ISNULL(All_t.completion_accrual_contract_amount,0) 
					- (
						(
							( ISNULL(All_t.interim_payment_progress_based_on_contract,0) * ISNULL(All_t.contract_amount,0) ) 
							+ ISNULL(All_t.progress_payment_change_order,0)
						) 
						/ ISNULL(All_t.interim_payment_progress_epd,0) - ISNULL(All_t.contract_amount,0)	
					  )
					- ISNULL(All_t.contract_amount,0)
			END
		
		,price_difference_at_progress_payment = -- Formül: Faturalı Başberi Hakediş Tutarı (FF Dahil, KDV Haric) - İlave İş (Hakediş) - Sözleşme (Hakediş)
			ISNULL(All_t.invoiced_progress_payment_amount_ipd_evat,0) 
			- ISNULL(All_t.progress_payment_change_order,0) 
			- (
				ISNULL(All_t.interim_payment_progress_based_on_contract,0) 
				* ISNULL(All_t.contract_amount,0)
			  )
	
		,progress_based_on_contract_amount = interim_payment_progress_based_on_contract

		,progress_incl_price_difference = -- Formül: (Sözleşme Hakediş + Fiyat Farkı Hakediş) / (Sözleşme Bedeli + Fiyat Farkı İş Sonu)
			CASE
				WHEN All_t.progress_payment_change_order = 0 THEN NULL
				ELSE 
           			(
               			(ISNULL(contract_amount,0)*ISNULL(interim_payment_progress_based_on_contract,0)) -- Sözleşme Hakediş
               			+(ISNULL(price_difference_at_progress_payment,0)) -- Fiyat Farkı Hakediş
           			) /
           			(
               			ISNULL(contract_amount,0) -- Sözleşme Bedeli
               			+(
       						ISNULL(completion_accrual_contract_amount,0)
						- (
								(
									(ISNULL(All_t.interim_payment_progress_based_on_contract,0) * ISNULL(All_t.contract_amount,0)) 
									+ ISNULL(All_t.progress_payment_change_order,0)
								)
								/ ISNULL(All_t.interim_payment_progress_epd,0) 
								- ISNULL(All_t.contract_amount,0)
							)
       						- ISNULL(contract_amount,0)
               			)
           			)
			END

		,progress_incl_co = -- Formül: (Sözleşme (Hakediş) + İlave İş (Hakediş)) / (Sözleşme Bedeli + İlave İş (İş Sonu))
			CASE 
				WHEN All_t.progress_payment_change_order = 0 THEN NULL
				ELSE 
					((ISNULL(All_t.contract_amount,0) * ISNULL(All_t.interim_payment_progress_based_on_contract,0)) 
					 + ISNULL(All_t.progress_payment_change_order, 0)) 
					/ (
						(
							(
								(ISNULL(All_t.interim_payment_progress_based_on_contract,0) * ISNULL(All_t.contract_amount,0)) 
								+ ISNULL(All_t.progress_payment_change_order, 0)
							) 
							/ ISNULL(All_t.interim_payment_progress_epd,0) - ISNULL(All_t.contract_amount,0)
						) + ISNULL(All_t.contract_amount,0)
					  )
			END
	
		,progress_incl_uninvoiced_progress_payment = NULL

		,progress_incl_price_difference_and_co = -- Formül: (Fiyat Farkı (Hakediş) + İlave İş (Hakediş) + Sözleşme (Hakediş)) / İş Sonu Tahakkuk Sözleşme Tutarı (FF Dahil, KDV Hariç)
			(
				(
					ISNULL(All_t.invoiced_progress_payment_amount_ipd_evat,0) 
					- ISNULL(All_t.progress_payment_change_order,0) 
					- (ISNULL(All_t.interim_payment_progress_based_on_contract,0) * ISNULL(All_t.contract_amount,0))
				) 
				+ ISNULL(All_t.progress_payment_change_order,0) 
				+ (ISNULL(All_t.contract_amount,0) * ISNULL(All_t.interim_payment_progress_based_on_contract,0))
			) 
			/ ISNULL(All_t.completion_accrual_contract_amount,0)

		,at_completion_change_order = -- Formül: (Sözleşme (Hakediş) + İlave İş (Hakediş)) / Hakediş İlerlemesi (Fiyat Farkı Hariç) - Sözleşme Bedeli 
			CASE 
				WHEN All_t.interim_payment_progress_epd = 0 THEN NULL
				ELSE
					(
						(ISNULL(All_t.interim_payment_progress_based_on_contract,0) * ISNULL(All_t.contract_amount,0)) 
						+ ISNULL(All_t.progress_payment_change_order,0)
					)
					/ ISNULL(All_t.interim_payment_progress_epd,0) 
					- ISNULL(All_t.contract_amount,0)
			END
	
		,advance_deductions_in_progress_payments = 
			CAST(ISNULL(total_advance_payment_amount,0) AS FLOAT)
			- CAST(ISNULL(remaining_advance_payment_amount,0) AS FLOAT)

		,cpi = CASE WHEN I_o.actual_man_hour = 0 THEN NULL ELSE I_o.earned_man_hour / I_o.actual_man_hour END
		,spi = CASE WHEN I_o.planned_man_hour = 0 THEN NULL ELSE I_o.earned_man_hour / I_o.planned_man_hour END 
		,total_man_hour_gross_building_area = I_o.total_man_hour / All_t.gba
		,I_o.earned_man_hour
		,I_o.actual_man_hour
		,I_o.planned_man_hour
		,remaining_man_hour = I_o.total_man_hour - I_o.earned_man_hour
		,I_o.total_man_hour
		/** Ali Can Tatar Bey'lerden At Completion MH'nin Total MH değerini vermesi gerektiği iletildi **/
		--,at_completion_man_hour = I_o.actual_man_hour + I_o.total_man_hour - I_o.earned_man_hour
		,at_completion_man_hour = I_o.total_man_hour
		,Po.renaissance_personnel
		,Po.subcontractor_personnel
		,Po.total_personnel
		,Po.renaissance_direct_personnel
		,Po.renaissance_indirect_personnel
		,Po.renaissance_support_personnel
		,Po.subcontractor_direct_personnel
		,Po.subcontractor_indirect_personnel
		,Po.subcontractor_support_personnel
		,Po.personnel_civil
		,Po.personnel_mechanical
		,Po.personnel_electrical

		-- Phase 2
		,All_t.progress_payment_notes
		,All_t.one_month_look_ahead
		,All_t.critical_path
		,All_t.total_bid_packages
		,All_t.planned_bid_packages
		,All_t.completed_bid_packages
		,All_t.critical_notes
		,All_t.subcontractor_notes
		,All_t.indirect_personnel
		,DENSE_RANK() OVER(PARTITION BY All_t.project_id, All_t.reporting_date ORDER BY All_t.db_upload_timestamp DESC) AS update_rank

	FROM {{source('stg_to_kpi','raw__to_kpi_t_dim_all')}} All_t 
		LEFT JOIN {{source('stg_dimensions','raw__dwh_t_dim_project')}} Prj ON All_t.[project_id] = Prj.[project_id]  
		LEFT JOIN IlerlemeOzet I_o ON All_t.[project_id] = I_o.[project_id]
								  AND All_t.[reporting_date] = I_o.[reporting_date]
		LEFT JOIN PersonelOzet Po ON All_t.[project_id] = Po.[project_id]
								 AND All_t.[reporting_date] = Po.[reporting_date] 
)

SELECT 
	[rls_region] = CAST([rls_region] AS nvarchar(255))
	,[rls_group] = CAST(CONCAT(COALESCE([rls_group],''),'_',COALESCE([rls_region],'')) AS nvarchar(255))
	,[rls_company] = CAST(CONCAT(COALESCE([rls_company],''),'_',COALESCE([rls_region],'')) AS nvarchar(255))
	,[rls_businessarea] = CAST(CONCAT(COALESCE([rls_businessarea],''),'_',COALESCE([rls_region],'')) AS nvarchar(255))
	,[group] = CAST([group] AS nvarchar(255))
	,[company] = CAST([company] AS nvarchar(255))
	,[business_area] = CAST([business_area] AS nvarchar(255))
	,[contractor] = CAST([contractor] AS nvarchar(255))
	,[status] = CAST([status] AS nvarchar(255))
	,[country] = CAST([country] AS nvarchar(255))
	,[city] = CAST([city] AS nvarchar(255))
	,[currency] = CAST([currency] AS nvarchar(255))
	,[project_name] = CAST([project_name] AS nvarchar(255))
	,[project_shortname] = CAST([project_shortname] AS nvarchar(20))
	,[location] = CAST([location] AS nvarchar(255))
	,[project_id] = CAST([project_id] AS nvarchar(255))
	,[data_entry_timestamp] = CAST([data_entry_timestamp] AS datetime)
	,[reporting_date] = CAST([reporting_date] AS datetime)
	,[employer] = CAST([employer] AS nvarchar(255))
	,[contract_amount] = CAST([contract_amount] AS decimal(18,2))
	,[contract_type] = CAST([contract_type] AS nvarchar(255))
	,[project_commencement_date] = CAST([project_commencement_date] AS datetime)
	,[project_finish_date] = CAST([project_finish_date] AS datetime)
	,[gba] = CAST([gba] AS decimal(18,2))
	,[budget_revision_period] -- = CAST([budget_revision_period] AS nvarchar(255))
	,[baseline_revision_date] = CAST([baseline_revision_date] AS datetime)
	,[completion_accrual_contract_amount] = CAST([completion_accrual_contract_amount] AS decimal(18,2))
	,[invoiced_progress_payment_amount_ipd_evat] = CAST([invoiced_progress_payment_amount_ipd_evat] AS decimal(18,2))
	,[invoice_pending_progress_payment] = CAST([invoice_pending_progress_payment] AS decimal(18,2))
	,[interim_payment_progress_epd] = CAST([interim_payment_progress_epd] AS decimal(8,6))
	,[interim_payment_progress_based_on_contract] = CAST([interim_payment_progress_based_on_contract] AS decimal(8,6))
	,[change_order] = CAST([at_completion_change_order] AS decimal(18,2))
	,[cash_recieved] = CAST([cash_recieved] AS decimal(18,2))
	,[at_completion_cash] = CAST([at_completion_cash] AS decimal(18,2))
	,[overdue_receivables] = CAST([overdue_receivables] AS decimal(18,2))
	,[total_recievables] = CAST([total_recievables] AS decimal(18,2))
	,[total_advance_payment_amount] = CAST([total_advance_payment_amount] AS decimal(18,2))
	,[remaining_advance_payment_amount] = CAST([remaining_advance_payment_amount] AS decimal(18,2))
	,[advance_payment_guarantee_letter_amount] = CAST([advance_payment_guarantee_letter_amount] AS decimal(18,2))
	,[advance_payment_guarantee_letter_end_date] = CAST([advance_payment_guarantee_letter_end_date] AS date)
	,[performance_guarantee_letter_amount] = CAST([performance_guarantee_letter_amount] AS decimal(18,2))
	,[performance_guarantee_letter_end_date] = CAST([performance_guarantee_letter_end_date] AS datetime)
	,[site_delivery_date] = CAST([site_delivery_date] AS datetime)
	,[contract_finish_date] = CAST([contract_finish_date] AS datetime)
	,[target_finish_date] = CAST([target_finish_date] AS datetime)
	,[contract_start_date] = CAST([contract_start_date] AS datetime)
	,[project_duration] = CAST([project_duration] AS int)
	,[progress_incl_price_difference] = CAST([progress_incl_price_difference] AS decimal(8,6))
	,[progress_incl_price_difference_and_uninvoiced_progress_payment] = CAST([progress_incl_price_difference_and_uninvoiced_progress_payment] AS decimal(8,6))
	,[progress_incl_co_and_uninvoiced_progress_payment] = CAST([progress_incl_co_and_uninvoiced_progress_payment] AS decimal(8,6))
	,[progress_incl_all] = CAST([progress_incl_all] AS decimal(8,6))
	,[physical_progress] = CAST([physical_progress] AS decimal(8,6))
	,[planned_progress] = CAST([planned_progress] AS decimal(8,6))
	,[time_progress] = CAST([time_progress] AS decimal(8,6))
	,[realized_duration] = CAST([realized_duration] AS int)
	,[remaining_duration] = CAST([remaining_duration] AS int)
	,[targeted_remaining_duration] = CAST([targeted_remaining_duration] AS int)
	,[contractual_remaining_duration] = CAST([contractual_remaining_duration] AS int)
	,[interim_payment_progress] = CAST([interim_payment_progress] AS decimal(8,6))
	,[cash_progress] = CAST([cash_progress] AS decimal(8,6))
	,[progress_payment_contract] = CAST([progress_payment_contract] AS decimal(18,2))
	,[price_difference_at_completion] = CAST([price_difference_at_completion] AS decimal(18,2))
	,[price_difference_at_progress_payment] = CAST([price_difference_at_progress_payment] AS decimal(18,2))
	,[progress_based_on_contract_amount] = CAST([progress_based_on_contract_amount] AS decimal(8,6))
	,[progress_incl_co] = CAST([progress_incl_co] AS decimal(8,6))
	,[progress_incl_uninvoiced_progress_payment] = CAST([progress_incl_uninvoiced_progress_payment] AS decimal(8,6))
	,[progress_incl_price_difference_and_co] = CAST([progress_incl_price_difference_and_co] AS decimal(8,6))
	,[advance_deductions_in_progress_payments] = CAST([advance_deductions_in_progress_payments] AS decimal(18,2))
	,[cpi] = CAST([cpi] AS decimal(8,6))
	,[spi] = CAST([spi] AS decimal(8,6))
	,[total_man_hour_gross_building_area] = CAST([total_man_hour_gross_building_area] AS decimal(8,6))
	,[earned_man_hour] = CAST([earned_man_hour] AS decimal(18,2))
	,[actual_man_hour] = CAST([actual_man_hour] AS decimal(18,2))
	,[planned_man_hour] = CAST([planned_man_hour] AS decimal(18,2))
	,[remaining_man_hour] = CAST([remaining_man_hour] AS decimal(18,2))
	,[total_man_hour] = CAST([total_man_hour] AS decimal(18,2))
	,[at_completion_man_hour] = CAST([at_completion_man_hour] AS decimal(18,2))
	,[renaissance_personnel] = CAST([renaissance_personnel] AS int)
	,[subcontractor_personnel] = CAST([subcontractor_personnel] AS int)
	,[total_personnel] = CAST([total_personnel] AS int)
	,[renaissance_direct_personnel] = CAST([renaissance_direct_personnel] AS int)
	,[renaissance_indirect_personnel] = CAST([renaissance_indirect_personnel] AS int)
	,[renaissance_support_personnel] = CAST([renaissance_support_personnel] AS int)
	,[subcontractor_direct_personnel] = CAST([subcontractor_direct_personnel] AS int)
	,[subcontractor_indirect_personnel] = CAST([subcontractor_indirect_personnel] AS int)
	,[subcontractor_support_personnel] = CAST([subcontractor_support_personnel] AS int)
	,[personnel_civil] = CAST([personnel_civil] AS int)
	,[personnel_mechanical] = CAST([personnel_mechanical] AS int)
	,[personnel_electrical] = CAST([personnel_electrical] AS int)
	,testing_and_commissioning_date =  CAST (NULL AS datetime) -- SONRA SİLİNECEK
	,cash_in = CAST (NULL AS decimal(18,2)) -- SONRA SİLİNECEK
	,cash_out =  CAST (NULL AS decimal(18,2) ) -- SONRA SİLİNECEK
	,cash_pool =  CAST (NULL AS decimal(18,2)) -- SONRA SİLİNECEK
	,accrual_progress_percentage =  CAST (NULL AS decimal(8,6)) -- SONRA SİLİNECEK
	,indirect_personnel_percentage =  CAST (NULL AS decimal(8,6)) -- SONRA SİLİNECEK

	-- Phase 2
	,progress_payment_notes = CAST(progress_payment_notes AS nvarchar(MAX))
	,one_month_look_ahead = CAST(one_month_look_ahead AS nvarchar(MAX))
	,critical_path = CAST(critical_path AS nvarchar(MAX))
	,total_bid_packages = CAST(total_bid_packages AS decimal(18,2))
	,planned_bid_packages = CAST(planned_bid_packages AS decimal(18,2))
	,completed_bid_packages = CAST(completed_bid_packages AS decimal(18,2))
	,critical_notes = CAST(critical_notes AS nvarchar(MAX))
	,subcontractor_notes = CAST(subcontractor_notes AS nvarchar(MAX))
	,indirect_personnel = CAST(indirect_personnel AS decimal(18,2))
FROM All_View_Raw
WHERE update_rank = 1

UNION ALL

/** Tek seferlik DB'ye basılmış olan RMORE sample datasıdır,
dim projects'te is_only_rmore kolonu bu sample proje için 1 olduğundan rmore'daki datamarta yansıyacak olup BI tarafına yansımayacaktır
**/
SELECT * FROM [aws_stage].[to_kpi].[stg__to_kpi_t_dim_all_rmoresample]


