{{
  config(
    materialized = 'table',tags = ['rmg_kpi']
    )
}}

select
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(eq.business_area, '_', rls_region),
	eq.company,
	eq.business_area,
	eq.equipment_group,
	eq.object_type,
	eq.equipment,
	eq.registered_owner,
	mn.maintenancenotification AS notification_number,
	CAST(qmih.ausvn AS datetime) + CAST(qmih.auztv AS datetime) AS failure_start_time,
	CAST(creationdate AS datetime) +
	CAST(
		 RIGHT('0' + CAST(PARSENAME(REPLACE(REPLACE(REPLACE(creationtime, 'PT', ''), 'H', '.'), 'M', '.') , 3) AS VARCHAR(2)), 2) 
		 + ':' +
		 RIGHT('0' + CAST(PARSENAME(REPLACE(REPLACE(REPLACE(creationtime, 'PT', ''), 'H', '.'), 'M', '.') , 2) AS VARCHAR(2)), 2)
		 + ':' +
		 RIGHT('0' + CAST(PARSENAME(REPLACE(REPLACE(REPLACE(creationtime, 'PT', ''), 'H', '.'), 'M', '.') , 1) AS VARCHAR(2)), 2)
	AS datetime) creation_time,
	CASE
		WHEN YEAR(notificationcompletiondate) <> '9999' THEN (
			CAST(notificationcompletiondate AS datetime) +
			CAST(
				 RIGHT('0' + CAST(PARSENAME(REPLACE(REPLACE(REPLACE(notificationcompletiontime, 'PT', ''), 'H', '.'), 'M', '.') , 3) AS VARCHAR(2)), 2) 
				 + ':' +
				 RIGHT('0' + CAST(PARSENAME(REPLACE(REPLACE(REPLACE(notificationcompletiontime, 'PT', ''), 'H', '.'), 'M', '.') , 2) AS VARCHAR(2)), 2)
				 + ':' +
				 RIGHT('0' + CAST(PARSENAME(REPLACE(REPLACE(REPLACE(notificationcompletiontime, 'PT', ''), 'H', '.'), 'M', '.') , 1) AS VARCHAR(2)), 2)
			AS datetime))
		ELSE NULL
	END notification_completion_time
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_maintenancenotification') }} mn
left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_qmih') }} qmih ON mn.maintenancenotification = SUBSTRING(qmih.qmnum, 5, LEN(qmih.qmnum))
left join {{ ref('stg__rmg_kpi_t_dim_equipmentlist') }} eq ON qmih.equnr = eq.equipment
left join {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON eq.company = dim_comp.RobiKisaKod
WHERE 1=1
    AND maintenanceworkcenterplant IN (N'RMGM')
    AND notificationtype = 'Z2'
    AND isdeleted = ''
