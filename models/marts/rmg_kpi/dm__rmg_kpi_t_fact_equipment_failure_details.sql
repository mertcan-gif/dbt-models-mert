{{
  config(
    materialized = 'table',tags = ['rmg_kpi']
    )
}}

WITH main_cte AS (
SELECT 
	maintenancenotification AS notification_number,
	notificationtype AS notification_type,
	maintnotificationcode,
	maintnotificationcodegroup,
	maintnotificationcatalog,
	iscompleted,
	CAST(notificationcompletiondate AS datetime) +
	CAST(
	     RIGHT('0' + CAST(PARSENAME(REPLACE(REPLACE(REPLACE(notificationcompletiontime, 'PT', ''), 'H', '.'), 'M', '.') , 3) AS VARCHAR(2)), 2) 
	     + ':' +
	     RIGHT('0' + CAST(PARSENAME(REPLACE(REPLACE(REPLACE(notificationcompletiontime, 'PT', ''), 'H', '.'), 'M', '.') , 2) AS VARCHAR(2)), 2)
	     + ':' +
	     RIGHT('0' + CAST(PARSENAME(REPLACE(REPLACE(REPLACE(notificationcompletiontime, 'PT', ''), 'H', '.'), 'M', '.') , 1) AS VARCHAR(2)), 2)
	AS datetime) notification_completion_time
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_maintenancenotification') }}
WHERE 1=1
    AND maintenanceworkcenterplant IN (N'RMGM')
    AND notificationtype = 'Z1'
    AND isdeleted = ''
)

SELECT
	dim_comp.rls_region,
	dim_comp.rls_group,
	dim_comp.rls_company,
	rls_businessarea = CONCAT(eq.business_area, '_', dim_comp.rls_region),
	eq.company,
	eq.business_area,
	eq.equipment,
	eq.object_type,
	eq.equipment_group,
	eq.registered_owner,
	main_cte.notification_number,
	qpct.kurztext AS failure_type,
	CAST(qmih.ausvn AS datetime) + CAST(qmih.auztv AS datetime) AS failure_start_time,
	CASE 
		WHEN iscompleted = 'X' AND qmih.ausbs <> '0000-00-00' THEN (CASE WHEN qmih.auztb = '24:00:00' THEN CAST(qmih.ausbs AS datetime) + CAST('23:59:00' AS datetime) ELSE CAST(qmih.ausbs AS datetime) + CAST(qmih.auztb AS datetime) END) --TRY_CAST(qmih.ausbs AS datetime) + TRY_CAST(qmih.auztb AS datetime)
		WHEN iscompleted = 'X' AND qmih.ausbs = '0000-00-00' THEN notification_completion_time
	END AS failure_end_time,
	cost.amount AS cost,
	cost.currency AS currency
FROM main_cte
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_qmih') }} qmih ON main_cte.notification_number = SUBSTRING(qmih.qmnum, 5, LEN(qmih.qmnum))
LEFT JOIN {{ ref('stg__rmg_kpi_t_dim_equipmentlist') }} eq ON qmih.equnr = eq.equipment
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_qpct') }} qpct ON main_cte.maintnotificationcode = qpct.code
											                            AND main_cte.maintnotificationcodegroup = qpct.codegruppe
											                            AND main_cte.maintnotificationcatalog = qpct.katalogart
LEFT JOIN (select [notification], sum(amount) as amount, currency from {{ ref('stg__rmg_kpi_t_fact_equipment_failure_cost') }} group by [notification], currency) cost ON main_cte.notification_number = cost.[notification]																		
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON eq.company = dim_comp.RobiKisaKod
WHERE (qpct.sprache = 'T' OR qpct.sprache is null)