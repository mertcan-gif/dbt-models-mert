{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
--RMHM2110 masraf yerinin yorum satırına alınmasının sebebi bu hesaba kayıt atılmaması gerektiği içindir. 
--İş birimi bu şekilde ilettiği için yoruma alınmıştır

SELECT
	 rls_region = cm.RegionCode
	,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
	,rls_company = acd.rbukrs + '_' + cm.RegionCode
	,rls_businessarea = acd.rbusa + '_' + cm.RegionCode
	,cm.KyribaGrup AS [group]
	,acd.rbukrs AS company
	,acd.gkont AS current_code
	,lfa1.name1 AS current_name
	,acd.rbusa AS business_area
	,acd.gjahr AS fiscal_year
	,cast(acd.bldat AS date) AS document_date
	,acd.matnr AS material_code
	,makt.maktx AS material_description
	,CASE 
		WHEN acd.rcntr = 'RMHH2000' THEN 'ANKARA BINA ORTAK GIDER'
		WHEN acd.rcntr = 'RMHH2010' THEN 'PORTAKAL'
		WHEN acd.rcntr = 'RMHH2020' THEN 'PROTOKOL'
		WHEN acd.rcntr = 'RMHH2030' THEN 'REFIKB'
		WHEN acd.rcntr = 'RMHH2040' THEN 'LAVIDA'
		WHEN acd.rcntr = 'RMHH2050' THEN 'MEKIK'
		WHEN acd.rcntr = 'RMHH2060' THEN 'REFIK112'
		WHEN acd.rcntr = 'RMHH2100' THEN 'ISTANBUL BINA ORTAK GIDER'
		--WHEN acd.rcntr = 'RMHH2110' THEN 'PIAZZA'
		WHEN acd.rcntr = 'RMHH2111' THEN 'PIAZZA-7'
		WHEN acd.rcntr = 'RMHH2112' THEN 'PIAZZA-8'
		WHEN acd.rcntr = 'RMHH2113' THEN 'PIAZZA-9'
		WHEN acd.rcntr = 'RMHH2120' THEN 'ISKULELERI'
		WHEN acd.rcntr = 'RMHH2130' THEN 'LEVENT199'
		WHEN acd.rcntr = 'RMHH3100' THEN 'RMH ARACILIK GIDER'
	END AS office_name
	,acd.belnr AS document_no
	,acd.buzei AS document_line_item
	,acd.racct AS general_ledger_account
	,REVERSE(SUBSTRING(REVERSE(txt50), 1, CHARINDEX('-', REVERSE(txt50)) - 1)) AS category
	,e.konnr AS contract_number
	,cast(e.netpr as money) * eur_value AS contract_eur_value
	,cast(e.netpr as money) * usd_value AS contract_usd_value
	,cast(e.netpr as money) * try_value AS contract_try_value
	,acd.ksl AS eur_value
	,acd.osl AS usd_value
	,acd.hsl AS try_value
FROM {{ ref('stg__s4hana_t_sap_acdoca') }} acd  
LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf ON bkpf.belnr = acd.belnr 
												AND bkpf.gjahr = acd.gjahr 
												AND acd.rbukrs = bkpf.bukrs 
LEFT JOIN (SELECT * FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_skat') }} WHERE ktopl='RONS' AND spras = 'T') ska ON acd.racct = ska.saknr 
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON acd.rbukrs = cm.RobiKisaKod
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekpo') }} e ON acd.matnr=e.matnr 
												AND e.bukrs = acd.rbukrs 
												AND e.ebelp = acd.ebelp 
												AND e.ebeln = acd.ebeln
LEFT JOIN {{ ref('dm__dimensions_t_dim_dailys4currencies') }} ds on CAST(acd.bldat AS date) = ds.date_value
																AND ds.currency = acd.rwcur
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa1 ON lfa1.lifnr = acd.gkont
LEFT JOIN (SELECT * FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_makt') }} WHERE spras = 'T') makt ON RIGHT(makt.matnr, '8') = acd.matnr 
WHERE (rcntr IN (
				'RMHH2000'
				,'RMHH2010'
				,'RMHH2020'
				,'RMHH2030'
				,'RMHH2040'
				,'RMHH2050'
				,'RMHH2060'
				,'RMHH2100'
				--,'RMHH2110'
				,'RMHH2111'
				,'RMHH2112'
				,'RMHH2113'
				,'RMHH2120'
				,'RMHH2130'
				)
			or (rcntr = 'RMHH3100' and aufnr <> ''))
	AND (aufnr = '' OR aufnr IN (
								'RMH-LAVIDA'
								,'RMH-LEVNT199'
								,'RMH-MEKIK'
								,'RMH-PIAZZA'
								,'RMH-PIAZZA7'
								,'RMH-PIAZZA8'
								,'RMH-PIAZZA9'
								,'RMH-PORTAKAL'
								,'RMH-PROTOKOL'
								,'RMH-REFIK112'
								,'RMH-REFIKB')) 
	AND bkpf.xreversing = 0
	AND bkpf.xreversed = 0  
	AND racct LIKE '7%'
	AND acd.blart <> 'CD'
	AND makt.maktx NOT IN (
								'RMH PERSONEL TUKETIM GIDERLERI-YEMEK S.'
								,'RMH TEMSIL AGIRLAMA'
								,'RMH IS SAGLIGI VE GUVENLIGI'
								,'RMH ARAC KASKO SIGORTASI'
								,'RMH ARAC TRAFIK SIGORTASI'
								,'RMH BASIM VE KOPYALAMA'
								,'RMH BILGISAYAR DONANIM MALZEMESI'
								,'RMH IDARI DANISMANLIKLAR'
								,'RMH NAKLIYE'
								,'RMH PERSONEL TUKETIM-YEMEK'
								,'RMH PERSONEL TUKETIM-YEMEK KARTI'
								)
	AND makt.maktx IS NOT NULL