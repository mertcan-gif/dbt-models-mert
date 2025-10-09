{{
  config(
    materialized = 'table',tags = ['rgy_kpi','rmore']
    )
}}

/* 
Date: 20250918
Creator: Oguzhan Ece
Report Owner: Damla Uzun
Explanation:Bu sorguda  RGY şirketinin arıza bildirim detaylarını görmek amaçlanmıştır.
*/


with finals as(
SELECT 
	viqmel.qmnum AS notification_id,
	viqmel.objnr AS object_id,
	tj02t.txt04 AS condition_code,
	tj02t.txt30 AS condition_description,
	CASE 
		WHEN jest.stat='I0068' THEN 'Opened'
		WHEN jest.stat='I0070' THEN 'In Progress'
		WHEN jest.stat='I0072' THEN 'Completed'
		ELSE jest.stat
	END jest_condition,
	tq80_t.qmart AS notification_type,
	tq80_t.qmartx AS notification_type_description,
	qmel.qmtxt AS breakdown_description,
	viqmel.equnr AS equipman,
	eqkt.eqktx AS equipman_description,
	viqmel.tplnr AS functional_location,
	iflotx.pltxt AS functional_location_description,
	viqmel.swerk AS notification_plant,
	t001w.name1 AS notification_plant_description,
	qmel.qmgrp AS code_group,
	qmel.qmcod AS code,
	qpct.kurztext as code_description,
	CAST(CASE
			WHEN viqmel.ausvn  IS NULL 
			THEN CONVERT(VARCHAR(19), GETDATE(), 120)
			WHEN SUBSTRING(viqmel.auztv, 3, 2)='24'
			THEN CONCAT(DATEADD(DAY,1,CAST(viqmel.ausvn AS DATE)),' ','00:',
				SUBSTRING(viqmel.auztv, 6, 2),':',SUBSTRING(viqmel.auztv, 9, 2))
			ELSE CONCAT(CAST(viqmel.ausvn AS DATE),' ',SUBSTRING(viqmel.auztv, 3, 2),':',
				SUBSTRING(viqmel.auztv, 6, 2),':',SUBSTRING(viqmel.auztv, 9, 2)) 
		END 
	AS DATETIME) AS breakdown_start_datetime,
	--viqmel.ausbs AS breakdown_end_date,
	CAST(CASE
			WHEN viqmel.ausbs  IS NULL 
			THEN CONVERT(VARCHAR(19), GETDATE(), 120)
			WHEN SUBSTRING(viqmel.auztb, 3, 2)='24'
			THEN CONCAT(DATEADD(DAY,1,CAST(viqmel.ausbs AS DATE)),' ','00:',
				SUBSTRING(viqmel.auztb, 6, 2),':',SUBSTRING(viqmel.auztb, 9, 2))
			ELSE CONCAT(CAST(viqmel.ausbs AS DATE),' ',SUBSTRING(viqmel.auztb, 3, 2),':',
				SUBSTRING(viqmel.auztb, 6, 2),':',SUBSTRING(viqmel.auztb, 9, 2)) 
	END 
	AS DATETIME) AS breakdown_end_datetime,
			qmel.ernam AS creator_info,
	ISNULL(CASE 
		WHEN viqmel.zzkullanim_disi='H' THEN 'No'
		WHEN viqmel.zzkullanim_disi='E' THEN 'Yes'
	END,'No') out_of_order,
	qmel.aenam AS last_modifier,
	CAST(viqmel.qmdat AS DATE) AS period
FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_viqmel') }} viqmel
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_qmel') }} qmel
		ON CAST(viqmel.qmnum AS INT)=CAST(qmel.qmnum AS INT)
	LEFT JOIN  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_jestrgy') }} jest
		ON viqmel.objnr=jest.objnr AND jest.inact='' and jest.stat in ('I0068','I0070','I0072')
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tj02t') }} tj02t
		ON tj02t.istat=jest.stat
		AND spras='T' 
	LEFT JOIN  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tq80_t') }} tq80_t 
		ON tq80_t.qmart=qmel.qmart
		AND tq80_t.spras='T' 
	LEFT JOIN  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_qmfe') }} qmfe 
		ON CAST(viqmel.qmnum AS INT)=CAST(qmfe.qmnum AS INT)
		AND qmfe.fenum=1 --?
	LEFT JOIN  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_qpct') }}  qpct
		ON qpct.katalogart=qmel.qmkat 
		AND qpct.codegruppe=qmel.qmgrp 
		AND qpct.code=qmel.qmcod
		AND sprache='T' 
	LEFT JOIN  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eqkt') }}  eqkt
		ON viqmel.equnr=eqkt.equnr
		AND eqkt.spras='TR' 
	LEFT JOIN   {{ source('stg_s4_odata', 'raw__s4hana_t_sap_iflotx') }}  iflotx
		ON viqmel.tplnr=iflotx.tplnr
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w 
		ON t001w.werks=viqmel.swerk	
WHERE 1=1
	AND viqmel.swerk like 'G%'
	AND viqmel.qmart='Z1' 
)

select 
	dim_projects.rls_region
	,dim_projects.rls_group
	,dim_projects.rls_company
	,dim_projects.rls_businessarea
	,rls_key=CONCAT(rls_businessarea,'-',rls_company,'-',rls_group)
	,finals.*
	,datediff(minute, breakdown_start_datetime, breakdown_end_datetime)  as sla_time_minutes
    ,round(cast(datediff(minute, breakdown_start_datetime, breakdown_end_datetime)/60.0 as float),2) as sla_time_hours
    ,round(cast(datediff(minute, breakdown_start_datetime, breakdown_end_datetime)/1440.0 as float),2) as sla_time_days
from finals 
LEFT JOIN {{ ref('dm__dimensions_t_dim_projects') }} dim_projects 
    ON finals.notification_plant = dim_projects.business_area