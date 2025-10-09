{{
  config(
    materialized = 'table',tags = ['nwc_kpi','duetoduefrom']
    )
}}
	
WITH RAW_DATA_FROM_DIM AS (
	SELECT
		company = mgd.bukrs
		,RBUSA = mgd.gsber
		,PostingDate = mgd.end_of_month
		,mgd.budget_currency
		,HSL = COALESCE(HSL,0)
		,KSL = COALESCE(KSL,0)
		,OSL = COALESCE(OSL,0)
		,mgd.AnaHesap
	FROM {{ ref('stg__nwc_kpi_t_dim_duetoduefromdimensionwithalldates') }} mgd
		LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_duetoduefromacdocaadjusted') }} aa ON aa.RBUSA = mgd.gsber
									AND aa.company = mgd.bukrs
									AND aa.PostingDate = mgd.end_of_month
									AND aa.AnaHesap = mgd.AnaHesap
	UNION ALL

	SELECT
		company
		,RBUSA
		,PostingDate = '2022-12-31'
		,budget_currency
		,HSL = COALESCE(HSL,0)
		,KSL = COALESCE(KSL,0)
		,OSL = COALESCE(OSL,0)
		,AnaHesap
	FROM {{ ref('stg__nwc_kpi_t_fact_duetoduefromacdocaadjusted') }} 	
	WHERE PostingDate < '2022-12-31'
)


--,CUMULATIVE_DATA AS (
	SELECT
		company
		,RBUSA
		,PostingDate
		,budget_currency
		,HSL
		,KSL
		,OSL
		,AnaHesap
		,SUM(HSL) over (partition by RBUSA, AnaHesap order by PostingDate) as cumulative_total_hsl
		,SUM(KSL) over (partition by RBUSA, AnaHesap order by PostingDate) as cumulative_total_ksl
		,SUM(OSL) over (partition by RBUSA, AnaHesap order by PostingDate) as cumulative_total_osl
	FROM RAW_DATA_FROM_DIM dt
--)

--SELECT
	--company
	--,RBUSA
	--,PostingDate
	--,budget_currency
	--,HSL
	--,KSL
	--,OSL
	--,AnaHesap
	--,CASE
	--	WHEN budget_currency = 'EUR' THEN cumulative_total_ksl
	--	WHEN budget_currency = 'USD' THEN cumulative_total_osl
	--	ELSE cumulative_total_hsl 
	--END AS cumulative_total
	
--FROM CUMULATIVE_DATA
