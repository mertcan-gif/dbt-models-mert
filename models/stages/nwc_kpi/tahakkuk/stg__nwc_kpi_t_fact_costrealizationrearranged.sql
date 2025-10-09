
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','tahakkuk']
    )
}}

	SELECT 
		company,--ACDOCA=> RBUKRS
		fiscal_year,
		document_number,
		document_line_item,
		general_ledger_account, --ACDOCA=> RACCT
		offsetting_account_number,
		business_area, --ACDOCA=> RBUSA
		posting_date,--ACDOCA=> BUDAT
		document_date, --ACDOCA=> BLDAT
		entry_date,
		day,--
		month,--
		year,--
		period,--
		amount_in_tl, --ACDOCA=> --s4.BSEG.HSL AS [BAKİYE TL], 
		amount_in_usd, --ACDOCA=> --s4.BSEG.HSL AS [BAKİYE TL], OSL
		amount_in_eur, --ACDOCA=> --s4.BSEG.HSL AS [BAKİYE TL],	KSL
		amount_in_bp = CASE
							WHEN TCURX.CURRDEC = 3 THEN amount_in_bp/10 
					   ELSE amount_in_bp END, 
		commitment_item,
		cost_center,
		cost_center_description,
		document_currency,
		item_text,
		document_type,
		level_1,
		level_2,
		level_3,
		level_4,
		level_1_definition,
		level_2_definition,
		level_3_definition,
		level_4_definition,
		vendor,
		business_area_description,
		[type], 
		awkey,
		[pyp_element],
		fiscal_period,
		is_adjusting_document = 'NO'


		,vendor_code
		,document_header_text
		,functional_area
		,functional_area_text
		,funds_center
		,material_number
		,material_description
		,contract_number
		,sas_short_text
		,purchasing_document
		,sas_amount
		,unit
		,net_price
		,warehouse_document_number
		,warehouse_amount
		,warehouse_unit
		,warehouse_material_description

	FROM {{ ref('stg__nwc_kpi_t_fact_costrealizationnotadjusted') }}
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON document_currency = TCURX.CURRKEY 