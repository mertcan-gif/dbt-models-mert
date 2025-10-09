{{
  config(
    materialized = 'table',tags = ['fi_kpi']
    )
}}

WITH quarter_ AS (
select 
	*
from (
	SELECT 
		date,
		CASE 
			WHEN MONTH(date) = 3 THEN date
			WHEN MONTH(date) = 6 THEN date
			WHEN MONTH(date) = 9 THEN date
			WHEN MONTH(date) = 12 THEN date
			ELSE NULL
		END AS end_of_quarter_month
	FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }}
	WHERE date = EOMONTH(date) ) subquery
where end_of_quarter_month is not null
	)

SELECT 
	[rls_region]
	,[rls_group]
	,[rls_company]
	,[rls_businessarea]
	,[company_code]
	,[company_name]
	,[actual_working_country]
	,[employment_type]
	,[collar_type]
	,[gender]
	,[employee_count]
	,[snapshot_date]
FROM {{ ref('stg__incremental_kpi_t_fact_employeeheadcount') }}
--2024 sonu icin ayin son gunune ait snapshot (goruntu) alinamadigi icin, 2024-12-30 tarihi filtreye eklenmistir.
--Bundan sonraki surecte, her ayin son gunu raw (ham) tablosuna eklenecektir. 
--Bu durum yalnizca 2024 yili sonu icin gecerli olup, sonraki yillarda ayni ozel duruma gerek duyulmayacaktir.
WHERE (snapshot_date IN (SELECT date FROM quarter_) 
		OR 
		snapshot_date = '2024-12-30')