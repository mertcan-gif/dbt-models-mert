
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','stockaging','stockturnoverraw']
    )
}}

	select 
		[rls_region]   = kuc.RegionCode 
		,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
		,[rls_company] = CONCAT(COALESCE(RBUKRS ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
		,[rls_businessarea] = CONCAT(COALESCE(RBUSA,''),'_',COALESCE(kuc.RegionCode,''))
		,rbukrs
		,rbusa
		,right(matnr,8) AS matnr
		,cast(bldat as date) as bldat
    ,case when tcurx.currdec = 3 THEN cast(hsl as decimal(18,2))/10 ELSE cast(hsl as decimal(18,2)) end as amount
		,rwcur
		,gkont
		,blart
    ,maliyet = case when tcurx.currdec = 3 and (blart = 'WA' or blart = 'WL') and (gkont like '740%' or gkont like '621%') then cast(hsl as decimal(18,2))/10 
                    when (tcurx.currdec <> 3 OR tcurx.currdec IS NULL) and (blart = 'WA' or blart = 'WL') and (gkont like '740%' or gkont like '621%') then cast(hsl as decimal(18,2))
                    else 0 end 
		{# ,maliyet = case when (blart = 'WA' or blart = 'WL') and (gkont like '740%' or gkont like '621%') then cast(ksl as decimal(18,2)) else 0 end #}
	from {{ ref('stg__s4hana_t_sap_acdoca') }} acdoca
    left join {{ ref('vw__s4hana_v_sap_ug_t001') }} t001 ON acdoca.rbukrs = t001.bukrs
    left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} tcurx ON t001.waers = tcurx.currkey 
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON acdoca.RBUKRS = kuc.RobiKisaKod
	where 1=1	
		and racct like '15%'
		and racct not like '159%'
    and matnr is not null




