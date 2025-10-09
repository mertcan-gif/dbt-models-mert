{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
/*
	Verilen avansların tutarları bulunmaktadır.
*/

/*
SAP ekibindeki Çagla Hanim'dan gelen bilgiye göre hrp1010 tablosunda bulunan hilfm kolonundaki
	001: Beyaz Yaka
	002: Mavi Yaka - Direkt
	003: Mavi Yaka - Endirekt olarak tanimlanmistir.

Fazla mesaileri hesaplamak için (men_hour - subcontractor_hour) yapilmalidir.
*/

WITH project_company_mapping AS (
  SELECT
    name1
    ,WERKS
    ,w.BWKEY
    ,bukrs
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} w
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} k ON w.bwkey = k.bwkey
  )

SELECT 
	c.rls_region,
	c.rls_group,
	c.rls_company,
	rls_businessarea = CONCAT(m.werks , '_' , c.rls_region),
	c.KyribaGrup as [group],
	m.bukrs as company,
    project_code = cslog.werks,
	project_name = t001w.name1,
	project_status = cd.status,
	bu_category = cd.category,
	personnel_no = cslog.pernr,
	personnel_type = 'Subcontractor',
	collar_type = 
				CASE
					WHEN hrp.hilfm = '001' THEN 'Beyaz Yaka' 
					WHEN hrp.hilfm IN ('002', '003') THEN 'Mavi Yaka'
					ELSE hrp.hilfm
				END,
	personnel_category =
					CASE
						WHEN hrp.hilfm IN ('001', '003') THEN 'Endirekt Personel' 
						WHEN hrp.hilfm = '002' THEN 'Direkt Personel'
						ELSE hrp.hilfm
					END,
    date = CAST(cslog.datum AS date),
	man_hour = CAST(cslog.ZZIKGRS AS float),
    subcontractor_no = cslog.zztaseronno,
	subcontractor_name = lfa.name1,
    subcontractor_hours = CAST(mesai.ZMESAISAAT AS float)
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_010_t_cs_log') }} cslog
	LEFT JOIN (
				SELECT 
					* 
				FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hrp1010') }}
				WHERE subty = '0001') hrp ON cslog.plans = hrp.objid
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_messaa') }} mesai ON mesai.zztaseronno = cslog.zztaseronno
																			AND mesai.WERKS = cslog.werks
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa ON lfa.lifnr = cslog.zztaseronno
	LEFT JOIN {{ ref('dm__to_kpi_t_dim_consolidateddata') }} cd ON cd.sap_business_area = cslog.werks
	LEFT JOIN project_company_mapping m ON cslog.werks= m.werks
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} c ON c.RobiKisaKod = m.bukrs
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON t001w.werks = cslog.werks
	LEFT JOIN (SELECT DISTINCT
						company 
						,project_code
						,subcontractor_no
				FROM {{ ref('stg__to_kpi_t_fact_progresspaymentsap') }} ) spp on spp.company = m.bukrs
																				AND spp.project_code = m.werks
																				AND spp.subcontractor_no = cslog.zztaseronno
WHERE cslog.zztaseronno <> '';
	--and spp.company IS NOT NULL;
