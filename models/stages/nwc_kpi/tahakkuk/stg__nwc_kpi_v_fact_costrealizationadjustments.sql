
{{
  config(
    materialized = 'view',tags = ['nwc_kpi','tahakkuk','cf_adj']
    )
}}

	SELECT
		company = bukrs 
		,fiscal_year = gjahr  
		,document_number = belnr  
		,document_line_item = ''
		,general_ledger_account = hkont 
		,offsetting_account_number = CAST(gkont AS nvarchar(MAX)) 
		,business_area = gsber 
		,posting_date = CAST(h_budat AS DATE) --
		,document_date = CAST(h_bldat AS DATE) -- 
		,entry_date = CAST(entrance AS date)
		,[day] 
		,[month] 
		,[year] 
		,[period]  
		,[amount_in_tl] = balance_tl 
		,[amount_in_usd] = balance_dollar
		,[amount_in_eur] = balance_euro
		,[amount_in_bp] = balance_bp
		,commitment_item =  CAST(fipos AS nvarchar(MAX)) 
		,cost_center = kostl 
		,cost_center_description = m_place_description 
		,document_currency = h_waers 
		,item_text = sgtxt 
		,document_type = h_blart 
		,level_1  
		,level_2  
		,level_3  
		,level_4  
		,level_1_definition = level_1_description 
		,level_2_definition = level_2_description 
		,level_3_definition = level_3_description 
		,level_4_definition = level_4_description 
		,vendor = name1 
		,business_area_description = gtext 
		,[type] = CASE 
					WHEN [type] = N'MERKEZ' THEN 'GIDER' ELSE [type]  
				END 
		,awkey 
		,[pyp_element] = cast(pyp_item as nvarchar)
		,fiscal_period = cast('' AS nvarchar)
		-- ,vendor_code = lifnr
		,is_adjusting_document = 'YES'
		
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_rtiaccrualadjustmentadjwtb') }}
	
	UNION ALL 

	SELECT
		bukrs 
		,gjahr  
		,belnr  
		,buzei = ''
		,hkont 
		,gkont = CAST(gkont AS nvarchar(MAX)) 
		,gsber 
		,h_budat = CAST(h_budat AS DATE) --
		,h_bldat = CAST(h_bldat AS DATE) -- 
		,[GİRİŞ] = CAST(entrance AS date)
		,[GÜN] = [day] 
		,[AY] = [month] 
		,[YIL] = [year] 
		,[DÖNEM] = [period]  
		,[BAKİYE TL] = balance_tl 
		,[BAKİYE DOLAR] = balance_dollar
		,[BAKİYE EURO] = balance_euro
		,[BAKİYE BP] = balance_bp
		,FIPOS =  CAST(fipos AS nvarchar(MAX)) 
		,KOSTL = kostl 
		,[M yeri tanımı] = m_place_description 
		,H_WAERS = h_waers 
		,SGTXT = sgtxt 
		,H_BLART = h_blart 
		,[1SEVİYE] = level_1  
		,[2SEVİYE] = level_2  
		,[3SEVİYE] = level_3  
		,[4SEVİYE] = level_4  
		,[1 SEVIYE TANIMI] = level_1_description 
		,[2 SEVIYE TANIMI] = level_2_description 
		,[3 SEVIYE TANIMI] = level_3_description 
		,[4 SEVIYE TANIMI] = level_4_description 
		,NAME1 = name1 
		,GTEXT = gtext 
		,tür2 = CASE 
					WHEN [type] = N'MERKEZ' THEN 'GIDER' ELSE [type]  
				END 
		,AWKEY = awkey 
		,[Pyp öğesi] = cast(pyp_item as nvarchar)
		,fiscal_period = cast('' AS nvarchar)
		-- ,vendor_code = lifnr
		,is_adjusting_document = 'YES'
		
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_rtiaccrualadjustmentdevir') }}
	
	UNION ALL 

	SELECT
		bukrs 
		,gjahr  
		,belnr  
		,buzei = ''
		,hkont 
		,gkont = CAST(gkont AS nvarchar(MAX)) 
		,gsber 
		,h_budat = CAST(h_budat AS DATE) --
		,h_bldat = CAST(h_bldat AS DATE) -- 
		,[GİRİŞ] = CAST(entrance AS date)
		,[GÜN] = [day] 
		,[AY] = [month] 
		,[YIL] = [year] 
		,[DÖNEM] = [period]  
		,[BAKİYE TL] = balance_tl 
		,[BAKİYE DOLAR] = balance_dollar
		,[BAKİYE EURO] = balance_euro
		,[BAKİYE BP] = balance_bp
		,FIPOS =  CAST(fipos AS nvarchar(MAX)) 
		,KOSTL = kostl 
		,[M yeri tanımı] = m_place_description 
		,H_WAERS = h_waers 
		,SGTXT = sgtxt 
		,H_BLART = h_blart 
		,[1SEVİYE] = level_1  
		,[2SEVİYE] = level_2  
		,[3SEVİYE] = level_3  
		,[4SEVİYE] = level_4  
		,[1 SEVIYE TANIMI] = level_1_description 
		,[2 SEVIYE TANIMI] = level_2_description 
		,[3 SEVIYE TANIMI] = level_3_description 
		,[4 SEVIYE TANIMI] = level_4_description 
		,NAME1 = name1 
		,GTEXT = gtext 
		,tür2 = CASE 
					WHEN [type] = N'MERKEZ' THEN 'GIDER' ELSE [type]  
				END 
		,AWKEY = awkey 
		,[Pyp öğesi] = cast(pyp_item as nvarchar)
		,fiscal_period = cast('' AS nvarchar)
		-- ,vendor_code = lifnr
		,is_adjusting_document = 'YES'
		
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_rtiaccrualadjustmentgeneral') }}
	
	UNION ALL 

	SELECT
		bukrs 
		,gjahr  
		,belnr  
		,buzei = ''
		,hkont 
		,gkont = CAST(gkont AS nvarchar(MAX)) 
		,gsber 
		,h_budat = CAST(h_budat AS DATE) --
		,h_bldat = CAST(h_bldat AS DATE) -- 
		,[GİRİŞ] = CAST(entrance AS date)
		,[GÜN] = [day] 
		,[AY] = [month] 
		,[YIL] = [year] 
		,[DÖNEM] = [period]  
		,[BAKİYE TL] = balance_tl 
		,[BAKİYE DOLAR] = balance_dollar
		,[BAKİYE EURO] = balance_euro
		,[BAKİYE BP] = balance_bp
		,FIPOS =  CAST(fipos AS nvarchar(MAX)) 
		,KOSTL = kostl 
		,[M yeri tanımı] = m_place_description 
		,H_WAERS = h_waers 
		,SGTXT = sgtxt 
		,H_BLART = h_blart 
		,[1SEVİYE] = level_1  
		,[2SEVİYE] = level_2  
		,[3SEVİYE] = level_3  
		,[4SEVİYE] = level_4  
		,[1 SEVIYE TANIMI] = level_1_description 
		,[2 SEVIYE TANIMI] = level_2_description 
		,[3 SEVIYE TANIMI] = level_3_description 
		,[4 SEVIYE TANIMI] = level_4_description 
		,NAME1 = name1 
		,GTEXT = gtext 
		,tür2 = CASE 
					WHEN [type] = N'MERKEZ' THEN 'GIDER' ELSE [type]  
				END 
		,AWKEY = awkey 
		,[Pyp öğesi] = cast(pyp_item as nvarchar)
		,fiscal_period = cast('' AS nvarchar)
		-- ,vendor_code = lifnr
		,is_adjusting_document = 'YES'
		
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_rtiaccrualadjustmentgyg') }}
	
	UNION ALL 

	SELECT
		bukrs 
		,gjahr  
		,belnr  
		,buzei = ''
		,hkont 
		,gkont = CAST(gkont AS nvarchar(MAX)) 
		,gsber 
		,h_budat = CAST(h_budat AS DATE) --
		,h_bldat = CAST(h_bldat AS DATE) -- 
		,[GİRİŞ] = CAST(entrance AS date)
		,[GÜN] = [day] 
		,[AY] = [month] 
		,[YIL] = [year] 
		,[DÖNEM] = [period]  
		,[BAKİYE TL] = balance_tl 
		,[BAKİYE DOLAR] = balance_dollar
		,[BAKİYE EURO] = balance_euro
		,[BAKİYE BP] = balance_bp
		,FIPOS =  CAST(fipos AS nvarchar(MAX)) 
		,KOSTL = kostl 
		,[M yeri tanımı] = m_place_description 
		,H_WAERS = h_waers 
		,SGTXT = sgtxt 
		,H_BLART = h_blart 
		,[1SEVİYE] = level_1  
		,[2SEVİYE] = level_2  
		,[3SEVİYE] = level_3  
		,[4SEVİYE] = level_4  
		,[1 SEVIYE TANIMI] = level_1_description 
		,[2 SEVIYE TANIMI] = level_2_description 
		,[3 SEVIYE TANIMI] = level_3_description 
		,[4 SEVIYE TANIMI] = level_4_description 
		,NAME1 = name1 
		,GTEXT = gtext 
		,tür2 = CASE 
					WHEN [type] = N'MERKEZ' THEN 'GIDER' ELSE [type]  
				END 
		,AWKEY = awkey 
		,[Pyp öğesi] = cast(pyp_item as nvarchar)
		,fiscal_period = cast('' AS nvarchar)
		-- ,vendor_code = lifnr
		,is_adjusting_document = 'YES'
		
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_rtiaccrualadjustmenthedge') }}
	
	UNION ALL 

	SELECT
		bukrs 
		,gjahr  
		,belnr  
		,buzei = ''
		,hkont 
		,gkont = CAST(gkont AS nvarchar(MAX)) 
		,gsber 
		,h_budat = CAST(h_budat AS DATE) --
		,h_bldat = CAST(h_bldat AS DATE) -- 
		,[GİRİŞ] = CAST(entrance AS date)
		,[GÜN] = [day] 
		,[AY] = [month] 
		,[YIL] = [year] 
		,[DÖNEM] = [period]  
		,[BAKİYE TL] = balance_tl 
		,[BAKİYE DOLAR] = balance_dollar
		,[BAKİYE EURO] = balance_euro
		,[BAKİYE BP] = balance_bp
		,FIPOS =  CAST(fipos AS nvarchar(MAX)) 
		,KOSTL = kostl 
		,[M yeri tanımı] = m_place_description 
		,H_WAERS = h_waers 
		,SGTXT = sgtxt 
		,H_BLART = h_blart 
		,[1SEVİYE] = level_1  
		,[2SEVİYE] = level_2  
		,[3SEVİYE] = level_3  
		,[4SEVİYE] = level_4  
		,[1 SEVIYE TANIMI] = level_1_description 
		,[2 SEVIYE TANIMI] = level_2_description 
		,[3 SEVIYE TANIMI] = level_3_description 
		,[4 SEVIYE TANIMI] = level_4_description 
		,NAME1 = name1 
		,GTEXT = gtext 
		,tür2 = CASE 
					WHEN [type] = N'MERKEZ' THEN 'GIDER' ELSE [type]  
				END 
		,AWKEY = awkey 
		,[Pyp öğesi] = cast(pyp_item as nvarchar)
		,fiscal_period = cast('' AS nvarchar)
		-- ,vendor_code = lifnr
		,is_adjusting_document = 'YES'
		
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_rtiaccrualadjustmentinterest') }}
	






