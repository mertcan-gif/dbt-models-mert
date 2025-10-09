{{
  config(
    materialized = 'table',tags = ['archived']
    )
}}

WITH raw_data AS (
/****
hesap numarası 3lü değilde çok haneli olanları otomatik olarak buldurup IN sorgusunun içine yazıyorum, kalanları 3lü kod ile mapliyorum.
****/

	select 
		acdoca.rbukrs,
		rbusa,
		kunnr,
		case
			when LEFT(racct,3) IN (
						select DISTINCT	LEFT(hesap,3) AS Hesap
							from {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_nwcaccountsmapping') }} 
							WHERE LEN(hesap)>3
			) THEN racct
			ELSE LEFT(racct,3)
		END AS nwc_mapping ,
		CAST(budat AS DATE) AS PostingDate,
		CAST(bldat AS DATE) AS DocumentDate,
		hsl_adjusted = case when tcurx.currdec = 3 THEN hsl/10 ELSE hsl END
		
	from {{ ref('stg__s4hana_t_sap_acdoca') }} acdoca 
		LEFT JOIN {{ ref('vw__s4hana_v_sap_ug_t001') }} t001 ON acdoca.rbukrs = t001.bukrs
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} tcurx ON t001.waers = tcurx.currkey 
	WHERE rbukrs NOT IN (select RobiKisaKod from dimensions.raw__dwh_t_dim_companymapping where KyribaGrup = 'RETGROUP')
)
,summerized_data AS (
	select 
		rbukrs
		,business_area_code = acd.rbusa
		,business_area = t001w.name1 
		,COALESCE(
			case
				when acd.nwc_mapping IN ('120') AND vl.business_area IS NOT NULL THEN 'Contract Receivables'
				when acd.nwc_mapping IN ('126') AND vl.business_area IS NOT NULL THEN 'Retention/Bonus Receivables'
				ELSE nwc.nwc_mapping END
			,'Non-NWC PL') AS nwc_mapping
		,Format(PostingDate,'yyyy-MM') posting_year_month
		,sum(hsl_adjusted) as sum_hsl
	from raw_data acd 
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_nwcaccountsmapping') }} nwc ON nwc.hesap = acd.nwc_mapping
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON acd.rbusa = t001w.WERKS
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtivendorlist') }} vl ON 
													acd.rbusa = vl.business_area
													AND acd.kunnr = vl.s4_customer_code
	GROUP BY 
		rbukrs 
		,acd.rbusa
		,t001w.NAME1
		,COALESCE(
			case
				when acd.nwc_mapping IN ('120') AND vl.business_area IS NOT NULL THEN 'Contract Receivables'
				when acd.nwc_mapping IN ('126') AND vl.business_area IS NOT NULL THEN 'Retention/Bonus Receivables'
				ELSE nwc.nwc_mapping END
			,'Non-NWC PL') 
		,Format(PostingDate,'yyyy-MM')
)

select 
	rbukrs
	,business_area
	,business_area_code
	,posting_year_month
	,nwc_mapping
	,sum(sum_hsl) 
		over(
			partition by 	
				rbukrs
				,business_area
				,nwc_mapping
			order by 
			posting_year_month ROWS UNBOUNDED PRECEDING
			) as running_total
	,currency = t001.WAERS
	,s4c.try_value
	,s4c.eur_value
from summerized_data s
	LEFT JOIN {{ ref('vw__s4hana_v_sap_ug_t001') }} t001 ON s.RBUKRS = t001.BUKRS
	LEFT JOIN {{ ref('stg__nwc_kpi_t_dim_monthlycurrencies') }} s4c ON s.posting_year_month= s4c.year_month
								AND t001.WAERS = s4c.currency

