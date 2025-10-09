{{
  config(
    materialized = 'table',tags = ['nwc_kpi','monthlyreport']
    )
}}

with raw_data_bonus_receivables AS (
/****
hesap numarası 3lü değilde çok haneli olanları otomatik olarak buldurup IN sorgusunun içine yazıyorum, kalanları 3lü kod ile mapliyorum.
****/

	select 
		acdoca.rbukrs,
		rbusa,
		kunnr,
		lifnr,
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
		hsl_adjusted = case when tcurx.currdec = 3 THEN hsl/10 ELSE hsl END,
		wsl,
		zuonr
		
	from {{ ref('stg__s4hana_t_sap_acdoca') }} acdoca 
		LEFT JOIN {{ ref('vw__s4hana_v_sap_ug_t001') }} t001 ON acdoca.rbukrs = t001.bukrs
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} tcurx ON t001.waers = tcurx.currkey 
	where 1=1
        and rbukrs = 'RMI'
		and (lifnr = 'GZT' OR kunnr = 'GZT')
		and left(racct,3) = '120'
		and (LEFT(RIGHT(acdoca.fiscyearper,6),2) not in ('13','14','15','16','00') OR LEFT(RIGHT(acdoca.fiscyearper,6),2) is null)
		and budat >= '20231201'
		and (acdoca.blart <> 'IA' OR acdoca.blart IS NULL)
)

select 
	rbukrs
	,business_area_code = acd.rbusa
	,business_area = t001w.name1 
	,nwc_mapping = 'Bonus Receivables'
	,EOMONTH(PostingDate) posting_year_month
	,sum(hsl_adjusted) as sum_hsl
	,sum(wsl) as wsl
from raw_data_bonus_receivables acd 
	LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_nwcaccountsmapping') }} nwc ON nwc.hesap = acd.nwc_mapping
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON acd.rbusa = t001w.WERKS
	LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtivendorlist') }} vl ON 
												acd.rbusa = vl.business_area
												AND acd.kunnr = vl.s4_customer_code
GROUP BY 
	rbukrs 
	,acd.rbusa
	,t001w.NAME1
	,EOMONTH(PostingDate)

