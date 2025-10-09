{{
  config(
    materialized = 'table',tags = ['io_kpi','rmore']
    )
}}

/* 
Date: 20250905
Creator: Oguzhan Ece
Report Owner: Metehan Kaymak - Inci Yavuz
Explanation:Bu sorguda çalışanların  ticket hakediş bilgisine ulaşılması ve belirlenen off günlerinde 
yapılacak kesintileri görmek amaçlanmıştır.
*/


WITH meal_allowance AS (
SELECT 
    rls_region = CASE 
                    WHEN (SELECT TOP 1 custom_region 
					FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp 
					WHERE grp.[group] = a_level.name_en) = 'TR' THEN 'TUR'
                    ELSE 'RUS' 
                END
    ,rls_group = UPPER((SELECT TOP 1 group_rls 
                        FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp 
                        WHERE grp.[group] = a_level.name_en))
    ,rls_company = UPPER(b_level.name_en)
    ,rls_businessarea = work_area_code
    ,CONCAT(employees.[name], ' ', employees.[surname]) AS full_name
    ,employees.[sap_id]
    ,employees.[user_id] as sf_user_id
    ,employees.[global_id]
    ,employees.[payroll_company]
    ,employees.[payroll_company_code]
    ,employees.[meal_allowance_tr]
    ,employees.[workplace_tr]
    ,employees.actual_location_name_tr as physical_location
    ,CURRENT_TIMESTAMP  as "reporting_date"
FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} as employees
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_a') }} a_level ON a_level.code = employees.[a_level_code]
    LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_b') }}  b_level ON b_level.code = employees.[b_level_code]
WHERE 1=1
    AND employee_status_tr = N'AKTİF'
    AND employee_type_name_tr <> N'Hayalet Kullanıcı' 
	)
,user_leave_dates_healt_detention_start AS (	
SELECT
	user_id,
	CASE 
		WHEN start_date<=DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) 
		THEN CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) as DATE)
		ELSE CAST(start_date AS DATE) 
	END leaveday_start,
		CASE WHEN end_date>= EOMONTH(GETDATE()) 
		THEN EOMONTH(GETDATE()) 
		ELSE CAST(end_date AS DATE) 
	END leaveday_end,
	time_type_details
FROM   {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employeetime_all') }}
WHERE 
	1=1
	AND time_type in ('P200','U505') 
	AND approval_status='APPROVED'
	AND CAST(quantity AS FLOAT)>=10
	AND start_date<DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) 
	AND end_date>=DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)
) 

,user_leave_dates_healt_detention_current AS (	
SELECT
	user_id,
	CASE 
		WHEN start_date<=DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) 
		THEN CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) as DATE)
		ELSE CAST(start_date AS DATE) 
	END leaveday_start,
		CASE WHEN end_date>= EOMONTH(GETDATE()) 
		THEN EOMONTH(GETDATE()) 
		ELSE CAST(end_date AS DATE) 
	END leaveday_end,
	time_type_details
FROM   {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employeetime_all') }}
WHERE 
	1=1
	AND time_type in ('P200','U505') 
	AND approval_status='APPROVED'
	--AND CAST(quantity AS FLOAT)>=10
	AND start_date<=EOMONTH(GETDATE()) 
	AND end_date>=DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)
) 
,user_leave_dates_healt_detention_end AS (	
SELECT
	user_id,
	CASE 
		WHEN start_date<=DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) 
		THEN CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) as DATE)
		ELSE CAST(start_date AS DATE) 
	END leaveday_start,
		CASE WHEN end_date>= EOMONTH(GETDATE()) 
		THEN EOMONTH(GETDATE()) 
		ELSE CAST(end_date AS DATE) 
	END leaveday_end,
	time_type_details
FROM   {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employeetime_all') }}
WHERE 
	1=1
	AND time_type in ('P200','U505') 
	AND approval_status='APPROVED'
	AND CAST(quantity AS FLOAT)>=10
	AND start_date<=EOMONTH(GETDATE()) 
	AND end_date>EOMONTH(GETDATE())
) 

,user_leave_dates_hd AS (
SELECT user_id,count(user) quantity
FROM user_leave_dates_healt_detention_current 
JOIN  {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} dt
		ON dt.date BETWEEN leaveday_start AND leaveday_end
		AND is_weekday=1
		AND is_holiday=0
		 
group by user_id
having count(user)>=10
)
,user_leave_dates_healt_detention_all AS (
SELECT * FROM user_leave_dates_healt_detention_start
UNION 
SELECT uldhdc.* FROM user_leave_dates_healt_detention_current uldhdc
JOIN user_leave_dates_hd  uld
	ON uld.user_id=uldhdc.user_id
UNION 
SELECT * FROM user_leave_dates_healt_detention_end
)
,user_leave_dates AS (
SELECT 
	user_id,
	CASE 
		WHEN start_date<=DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) 
		THEN CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) AS DATE)
		ELSE CAST(start_date AS DATE) 
	END leaveday_start,
		CASE WHEN end_date>= EOMONTH(GETDATE()) 
		THEN EOMONTH(GETDATE()) 
		ELSE CAST(end_date AS DATE) 
	END leaveday_end,
	time_type_details
FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employeetime_all') }}
WHERE 
	1=1
	AND time_type in ('P142','P149','U400','U500','U501') 
	AND approval_status='APPROVED'
	AND start_date<=EOMONTH(GETDATE()) 
	AND end_date>=DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) 

UNION ALL
		

SELECT 
	uldhda.* 
FROM user_leave_dates_healt_detention_all uldhda

)
,leavedays_reason_dates AS (
SELECT 
	user_id,
	string_agg(uld.time_type_details,'-') leave_reason,
	min(uld.leaveday_start) leaveday_start
	,max(uld.leaveday_end) leaveday_end
FROM user_leave_dates uld
GROUP BY user_id
)
,leavedays_count AS (
SELECT 
	user_id,count(*) leave_days 
FROM user_leave_dates uld
	JOIN  {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} dt
		ON dt.date BETWEEN uld.leaveday_start AND uld.leaveday_end
		AND is_weekday=1
		AND is_holiday=0
GROUP BY user_id
)
SELECT 
    [rls_region]
    ,rls_group = CONCAT(COALESCE([rls_group],''),'_',COALESCE([rls_region],''))
	,rls_company = CONCAT(COALESCE([rls_company],''),'_',COALESCE([rls_region],''))
	,rls_businessarea = CONCAT(COALESCE([rls_businessarea],''),'_',COALESCE([rls_region],''))
    ,full_name
    ,[sap_id]
    ,sf_user_id
    ,[global_id]
    ,[payroll_company]
    ,[payroll_company_code]
    ,[meal_allowance_tr]
    ,[workplace_tr]
    ,physical_location
	,lr.leave_reason
	,lr.leaveday_start
	,lr.leaveday_end
	,COALESCE(lc.leave_days,0) AS this_month_unpaid_days
    ,reporting_date
FROM meal_allowance ma
	LEFT JOIN leavedays_count lc
		ON ma.[sf_user_id]=lc.user_id
	LEFT JOIN leavedays_reason_dates lr
		ON ma.[sf_user_id]=lr.user_id
WHERE ma.meal_allowance_tr=N'ALIR'