{{
  config(
    materialized = 'view',tags = ['to_kpi']
    )
}}
/*
Bu View'da, bir önceki hafta verisi DB'ye basılmamış olan projeler tespit edilir.
ED'da verisi basılmamış bu haftaların verilerinin boş gözükmemesi için, 
bir önceki haftanın verileri de bu haftanın verileri olarak gösterilir 
*/
WITH sorted_cte AS(
/*
Bu cte ProcurementDetails tablosunun (project_id bazında),
reporting_date ve data_entry_timestamp'e göre en güncel verilerini gösterir (RANK_NUM = 1)
*/
	SELECT 
		*
		,DENSE_RANK() OVER(PARTITION BY project_id ORDER BY reporting_date DESC, data_entry_timestamp DESC) AS RANK_NUM
	FROM {{source('stg_to_kpi','raw__to_kpi_t_fact_procurementdetails')}}  
),

projects_with_current_report AS (
/*
Bu cte ProcurementDetails tablosunda veri eksiği bulunmayan project_id'leri gösterir
*/
	SELECT DISTINCT project_id FROM sorted_cte
	WHERE 1=1
		AND RANK_NUM = 1
		AND DATEPART(WEEKDAY, reporting_date) = 6
		AND reporting_date < GETDATE()
		AND reporting_date > DATEADD(WEEK,-1,GETDATE())
),

projects_without_current_report AS (
/*
Bu cte ProcurementDetails tablosunda, bir önceki hafta verisi basılmamış olan project_id'leri gösterir
*/
	SELECT 
		DISTINCT project_id 
	FROM {{source('stg_to_kpi','raw__to_kpi_t_fact_procurementdetails')}} 
	WHERE 1=1
		AND project_id NOT IN (SELECT * FROM projects_with_current_report)
)

/*
Bu sorguda, son hafta güncel verisi bulunmayan projenin verileri,
en son DB'ye yüklenmiş olan verileri gösterecek şekilde güncellenir.
*/

SELECT

[project_id]
      ,[data_entry_timestamp] = GETDATE()
      ,[reporting_date] = DATEADD(DAY,7,[reporting_date])
      ,[db_upload_timestamp] = GETDATE()
      ,[bid_package]
      ,[start_date]
      ,[end_date]
      ,[duration]
	  ,[order_rank]
FROM sorted_cte
WHERE 1=1
	AND project_id IN (SELECT * FROM projects_without_current_report)
	AND RANK_NUM = 1

