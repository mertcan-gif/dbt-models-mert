{{
  config(
    materialized = 'table',tags = ['rmg_kpi']
    )
}}

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(iloa.gsber,'_', rls_region),
	bukrs AS company,
	equz.iwerk,
	iloa.gsber AS business_area,
	t001w.name1 AS business_area_desc,
	equi.equnr AS equipment_number,
	age_of_equipment = CASE WHEN baujj <> ' ' THEN YEAR(GETDATE()) - baujj ELSE NULL END,
	zzitoaracaktifpasif AS activity,
	zzcalismagrup AS working_group,
	zzitoeqdgrtrf AS traffic_equipment_value,
	zzitopolnotrf AS traffic_policy_number,
	zzitoprmbdtrf AS traffic_insurance_premium,
	IIF(zzitobegdatrf = '', NULL, CAST(zzitobegdatrf AS date)) AS traffic_start_date,
	IIF(zzitoenddatrf = '', NULL, CAST(zzitoenddatrf AS date)) AS traffic_end_date,
	zzitosgrfirm AS traffic_insurer,
	zzitoeqdgrksk AS comprehensive_equipment_value,
	zzitopolnoksk AS comprehensive_policy_number,
	zzitoprmbdksk AS comprehensive_insurance_premium,
	IIF(zzitobegdaksk = '', NULL, CAST(zzitobegdaksk AS date)) AS comprehensive_start_date,
	IIF(zzitoenddaksk = '', NULL, CAST(zzitoenddaksk AS date)) AS comprehensive_end_date,
	zzitokskfirm AS comprehensive_insurer,
	zzitoeqdgrmkr AS damage_equipment_value,
	zzitopolnomkr AS damage_policy_number,
	zzitoprmbdmkr AS damage_insurance_premium,
	IIF(zzitobegdamkr = '', NULL, CAST(zzitobegdamkr AS date)) AS damage_start_date,
	IIF(zzitoenddamkr = '', NULL, CAST(zzitoenddamkr AS date)) AS damage_end_date,
	IIF(zzitomynend = '', NULL, CAST(zzitomynend AS date)) AS inspection_end_date
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equi') }} equi
LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_fmfctr" f ON equi.equnr = f.fictr
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equz') }} equz ON equi.equnr = equz.equnr
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_iloa') }} iloa ON equz.iloan = iloa.iloan
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON iloa.gsber = t001w.werks
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON f.bukrs = dim_comp.RobiKisaKod
WHERE 1=1
AND eqtyp = 'T'
AND equz.datbi = '99991231'
AND equz.iwerk = 'RMGM'