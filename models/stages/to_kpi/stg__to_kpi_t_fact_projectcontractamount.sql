{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
WITH contract_amount_raw_data as (
	SELECT
		ekpo.bukrs as company
		,ekpo.werks as project 
		,ekko.zzctr_aszno AS contract_no
		,ekko.zzctr_resno
		,ekko.zzctr_haked
		,ekko.lifnr
		,ekpo.ebeln
		,ekko.zzbeltn
		,ekpo.ktmng 
		,konp.kbetr
		,ekko.lifnr AS subcontractor_no
		,contract_amount = CAST(CAST(ekpo.ktmng as float) * CAST(konp.kbetr as float) AS money) 
		,konp.konwa currency
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekpo') }} ekpo
	LEFT JOIN (
				SELECT 
					evrtn
					, evrtp
					, knumh
				FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_a016') }} WHERE datbi = '9999-12-31') a016 ON ekpo.ebeln = a016.evrtn
																								                                                    AND ekpo.ebelp = a016.evrtp
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekko') }} ekko ON ekko.ebeln = ekpo.ebeln
	LEFT JOIN (SELECT 
					kbetr
					,knumh
					,konwa 
				FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_konp') }} WHERE loevm_ko = '') konp ON konp.knumh = a016.knumh
	WHERE 1=1
		and ekpo.loekz <> 'L'
		and ekko.zzctr_haked = '1'
	)
	SELECT 
		company,
		project,
		contract_no,
		subcontractor_no,
		SUM(CASE WHEN currency = 'TRY' THEN contract_amount ELSE 0 END) AS contract_amount_try,
		SUM(CASE WHEN currency = 'USD' THEN contract_amount ELSE 0 END) AS contract_amount_usd,
		SUM(CASE WHEN currency = 'EUR' THEN contract_amount ELSE 0 END) AS contract_amount_eur
	FROM 
contract_amount_raw_data
	WHERE 1=1
		AND contract_no <> '' 
		AND contract_no  IS NOT NULL
	GROUP BY 
		subcontractor_no
		,contract_no
		,company
		,project