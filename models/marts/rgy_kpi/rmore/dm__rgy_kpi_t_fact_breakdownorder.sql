{{
  config(
    materialized = 'table',tags = ['rgy_kpi','rmore']
    )
}}

/* 
Date: 20250918
Creator: Oguzhan Ece
Report Owner: Damla Uzun
Explanation:Bu sorguda  RGY şirketinin arıza sipariş detaylarını görmek amaçlanmıştır.
*/
with finals as(
SELECT 
	CAST(aufk.aufnr AS INT) AS order_id,
	aufk.objnr AS object_id,
	tj02t.txt04 AS condition_code,
	tj02t.txt30 AS condition_description,
	CASE 
		WHEN jest.stat='I0001' THEN 'Opened'
		WHEN jest.stat='I0002' THEN 'In Progress'
		WHEN jest.stat='I0046' THEN 'Completed'
	ELSE jest.stat
	END jest_condition,
	CASE 
		WHEN t30.estat is not null THEN t30.txt30
		ELSE 'BO Siparisi Tamamlanmadi'
	END BO_condition,
	t003p.auart AS order_type,
	t003p.txt AS order_type_description,
	aufk.ktext AS order_description,
	CASE 
				WHEN TRY_CONVERT(int,afih.equnr) is not null
				THEN RIGHT(afih.equnr,8)
				ELSE afih.equnr
	END AS equipment,
	eqkt.eqktx AS equipment_description,
	iloa.tplnr AS functional_location,
	iflotx.pltxt AS functional_location_description,
	iloa.swerk AS order_plant,
	t001w.name1 AS BO_order_plant,
	aufk.aenam AS creator_info,
	afih.addat AS period
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_aufk') }} aufk
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_jestrgy') }} jest
		ON aufk.objnr=jest.objnr AND jest.inact='' and jest.stat in ('I0001','I0002','I0046')
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_jestrgy') }} jestbo
		ON aufk.objnr=jestbo.objnr AND jestbo.inact='' and jestbo.stat in ('E0002')
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tj02t') }} tj02t
		ON tj02t.istat=jest.stat
		AND spras='T' 
	
	LEFT JOIN  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tj30t') }} t30
		ON t30.estat=jestbo.stat
		AND t30.spras='T' 
		AND t30.txt04='SPTM'
	
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t003p') }} t003p
		ON t003p.auart=aufk.auart
		AND t003p.spras='T' 
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_afih') }} afih
		ON afih.aufnr=aufk.aufnr
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_iloa') }} iloa
		ON iloa.iloan=afih.iloan
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eqkt') }} eqkt
		ON CASE 
				WHEN TRY_CONVERT(int,afih.equnr) is not null
				THEN RIGHT(afih.equnr,8)
				ELSE afih.equnr
		   END = eqkt.equnr
		AND eqkt.spras='TR' --?
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w
		ON t001w.werks=iloa.swerk	
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_iflotx') }} iflotx
		ON iloa.tplnr=iflotx.tplnr
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp 
		ON aufk.bukrs = dim_comp.RobiKisaKod
WHERE 1=1
	AND aufk.werks LIKE 'G%'
	AND aufk.AUART IN ('ZPM1','ZPM2','ZPM6')
)


select 
	dim_projects.rls_region
	,dim_projects.rls_group
	,dim_projects.rls_company
	,dim_projects.rls_businessarea
	,rls_key=CONCAT(rls_businessarea,'-',rls_company,'-',rls_group)
	,finals.*
from finals 
LEFT JOIN {{ ref('dm__dimensions_t_dim_projects') }} dim_projects 
    ON finals.order_plant = dim_projects.business_area