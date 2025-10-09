{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}


/*
	Temmuz 2025 yılında yeni SF sistemi olan Rpeople geçişi ile beraber, o tarihe kadar kişilerin kullanıdğı tüm izinlerin toplamı toplu olarak atılmıştır.
	Örneğin bir personel 
		* 2024-01-06'de -1 gün
		* 2024-01-07'de -1 gün
		* 2024-01-08'de -1 gün
		* 2024-01-09'de -1 gün
		* 2024-07-17'de -1 gün
		* 2024-07-18'de -1 gün
		* 2024-07-19'de -1 gün
	izin kullandı ise, tek tek bugünler izin kullandı şeklinde değil. dip toplam olarak 2025-07-01 tarihinde -7 gün olarak atılmıştır. Bu sebeple sadece geçmiş sistemin verisi alındığında historic daily izin kullanım verisi kaybedilmektedir. Bu minvalde aşağıda yeni_sistem_scope_out cte'sinde bu atılan açılış kayıtları tespit edilerek filter out edilmiş eski sistemle unionlanarak historic veri kaybedilmeden korunmuştur.

	employee_time verisinde created_date_time iznin yaratılma tarihini tutmaktadır, eğer bir izin verisi employee_time da yok ise bunun yaratılma tarihini time_account_detail tablosundan almaktayız, kabulümüz bu yönde yapılmıştır.
*/

WITH eski_sistem as (
	select 
		'Coach' as sf_system
		,ta.user_id
		,emp.sap_id
		,leave_code = tad.external_code
		,leave_type = N'Yıllık İzin'
		,ta.external_code as time_account_external_code
		,calendar_entry_code = tad.calendar_entry
		,booking_date
		,cast(booking_amount as float) quantity
		,case 
			when booking_type = 'ACCRUAL' then N'İzin Hakedişi' 
			when booking_type = 'EMPLOYEE_TIME' then N'İzin Kullanımı' 
			else N'Manuel Adjustment Kaydı' end as tur
		,coalesce(et.created_date_time,tad.created_date_time) as created_date_time
		,comment
		from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_timeaccount') }} ta
			left join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_timeaccountdetails') }} tad ON tad.time_account_external_code = ta.external_code
			left join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employee_time_calender') }} etc ON tad.calendar_entry = etc.external_code
			left join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employee_time_all') }} et on et.[external_code] = etc.[employee_time_external_code]
			left join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }} emp on ta.user_id = emp.user_id

	where 1=1
		and account_type = N'0100'
		and booking_date < '2025-07-05'
		and account_closed = 0
),
yeni_sistem_scope_out as (
	select distinct
		tad.external_code,
		tad.time_account_external_code
		from "aws_stage"."sf_odata"."raw__hr_kpi_t_sf_newsf_timeaccount" ta
			RIGHT join "aws_stage"."sf_odata"."raw__hr_kpi_t_sf_newsf_timeaccountdetails" tad ON tad.time_account_external_code = ta.external_code
			left join "aws_stage"."sf_odata"."raw__hr_kpi_t_sf_newsf_employeetimecalender" etc ON etc.external_code = tad.calendar_entry
			left join aws_stage.sf_odata.raw__hr_kpi_t_sf_newsf_employeetime_all et on etc.employee_time_external_code = et.external_code
			left join sf_odata.raw__hr_kpi_t_sf_newsf_employees emp on emp.user_id = ta.user_id
	where 1=1
		and tad.created_date_time < '2025-07-23 23:59:59.000' -- bu tarihten sonra yaratılanlar 
	    and booking_date < '2025-07-05'
		and account_type = '0100'
		and (booking_type = 'ACCRUAL' or booking_type = 'MANUAL_ADJUSTMENT')
		and emp.employee_status_en = N'Active'
)
,yeni_sistem as (
	select 
		'Rpeople' as sf_system
		,ta.user_id
		,emp.sap_id
		,leave_code = tad.external_code
		,leave_type = N'Yıllık İzin'
		,ta.external_code as time_account_external_code
		,calendar_entry_code = tad.calendar_entry
		,booking_date
		,cast(booking_amount as float) quantity
		,case 
			when booking_type = 'ACCRUAL' then N'İzin Hakedişi' 
			when booking_type = 'EMPLOYEE_TIME' then N'İzin Kullanımı' 
			else N'Manuel Adjustment Kaydı' end as tur
		,coalesce(et.created_date_time,tad.created_date_time) as created_date_time
		,comment
		from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_timeaccount') }} ta
			left join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_timeaccountdetails') }} tad ON tad.time_account_external_code = ta.external_code
			left join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employeetimecalender') }} etc ON tad.calendar_entry = etc.external_code
			left join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employeetime_all') }} et on et.[external_code] = etc.[employee_time_external_code]
			left join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} emp on ta.user_id = emp.user_id
	where 1=1
		and account_type = N'0100'
		and booking_date <= DATEFROMPARTS(year(getdate()),12,31)
		and account_closed = 0
)

/* ,harcanan_gelecek_manuel as (
	SELECT 
		e.user_id
		,null as leave_code
		,N'Yıllık İzin' as leave_type
		,null as ta_external_code
		,null as calendar_entry_code
		,d.[date] as booking_date
		,-1 as booking_quantity
		,tur = N'İzin Kullanımı (Forecast)'
		,null as created_date_time
		,'DWH ekibinin forecast için yarattığı virtual kayıttır' as comment
	FROM 
		{{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }} e
	CROSS APPLY 
		(VALUES 
			(CAST('2025-07-14' AS DATE)),
			(CAST('2025-08-18' AS DATE)),
			(CAST('2025-08-19' AS DATE)),
			(CAST('2025-08-20' AS DATE)),
			(CAST('2025-08-21' AS DATE)),
			(CAST('2025-08-22' AS DATE)),
			(CAST('2025-08-25' AS DATE)),
			(CAST('2025-08-26' AS DATE)),
			(CAST('2025-08-27' AS DATE)),
			(CAST('2025-08-28' AS DATE)),
			(CAST('2025-08-29' AS DATE)),
			(CAST('2025-10-27' AS DATE)),
			(CAST('2025-10-28' AS DATE))
		) d([date])
	WHERE 
		e.employee_status_en = 'Active'
		and d.[date] > getdate()
) 
*/

,final_cte as (
	SELECT 
		eski_sistem.*,
		'normal_kayit' as transaction_type
	FROM eski_sistem
	UNION ALL
	SELECT 
		yeni_sistem.*, 
		transaction_type =
		case 
			when yeni_sistem_scope_out.external_code IS NOT NULL THEN N'acilis_kaydi' 
			else 'normal_kayit' 
		end 
	FROM yeni_sistem
		left join yeni_sistem_scope_out on 
				yeni_sistem.time_account_external_code = yeni_sistem_scope_out.time_account_external_code
			and	yeni_sistem.leave_code = yeni_sistem_scope_out.external_code
)

select 
      [user_id] = sap_id
	  ,sf_system
      ,[sap_id]
      ,[leave_code]
      ,[leave_type]
      ,[time_account_external_code]
      ,[calendar_entry_code]
      ,[booking_date]
      ,[quantity]
      ,[tur]
      ,[created_date_time]
      ,[comment]
      ,transaction_type
from final_cte