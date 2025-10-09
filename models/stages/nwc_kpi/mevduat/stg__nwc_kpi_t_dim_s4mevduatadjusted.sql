{{
  config(
    materialized = 'table',tags = ['fi_kpi','s4mevduat']
    )
}}

WITH DATE_TABLE as
(
	SELECT 
	CAST(DATEADD(dd, number, '2022-12-31') AS DATE) Date
	FROM 
	master..spt_values m1
	WHERE 
	type = 'P' 
	AND DATEADD(dd, number, '2022-12-31') <= GETDATE()
)

,DIMENSION_WITH_ALL_DATES AS (
    SELECT 
		s.RBUKRS 
		,s.RACCT
		,c.Date AS date
    FROM (
			  SELECT 
				rd.RBUKRS
				,rd.RACCT
				,MIN(rd.DATE) AS min_date
				,MAX(rd.DATE) AS max_date
			  FROM {{ ref('stg__nwc_kpi_t_fact_s4mevduatraw') }} rd
			  GROUP BY rd.RBUKRS, rd.RACCT
		) s
    CROSS JOIN DATE_TABLE c
    WHERE c.Date >= s.min_date 
)

,FACT_WITH_NULLS AS (
	SELECT 
		dd.*
		,rd.RunningTotal
		,COUNT(RunningTotal) over (partition by dd.RBUKRS, dd.RACCT order by dd.DATE) as grp
	FROM DIMENSION_WITH_ALL_DATES dd
			LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_s4mevduatraw') }} rd ON rd.RBUKRS = dd.RBUKRS
							 AND rd.RACCT = dd.RACCT
							 AND rd.DATE = dd.date
)

,TOTALS_DIM AS (
	SELECT DISTINCT
		RBUKRS
		,RACCT
		,grp
		,RunningTotal
	FROM FACT_WITH_NULLS
	WHERE RunningTotal IS NOT NULL
)

SELECT 
	gt.[DATE]
	,rd.[NONSAP]
	,gt.[RBUKRS]
	,gt.[RACCT]
	,rd.[TXT20]
	,rd.[TXT50]
	,rd.[RTCUR]
	,td.[RunningTotal]
	,rd.[ULKE]
	,rd.[GRUPORANI]
	,rd.[GRUP]
	,rd.[SEKTOR]
	,rd.[ALTSEKTOR]
	,rd.[SERBEST]
	,rd.[ULKE_BANKA]
	,rd.[BANKATANIMI]
	,rd.[HESAP_TIPI_TANIMI]
	,rd.[KREDIGRUBU]
	,rd.[CONTRIBUTEGROUP]
	,rd.[YK_SEKTOR]
	,rd.[YK_ULKE]
	,rd.[YK_KREDIGRUBU]
	,rd.[KREDIKATEGORISI]
	,rd.[YK_KREDIKISITI]
	,rd.[KA_KREDIGRUBU]
	,rd.[KA_KREDIKISITI]
	,rd.[CASH_GRUP1]
	,rd.[CASH_GRUP2]
	,rd.[CASH_GRUP3]
	,rd.[CASH_GRUP4]
FROM FACT_WITH_NULLS gt
	LEFT JOIN TOTALS_DIM td ON td.grp = gt.grp
						   AND td.RBUKRS = gt.RBUKRS
						   AND td.RACCT = gt.RACCT
	LEFT JOIN {{ ref('stg__nwc_kpi_t_dim_s4mevduataccountdetails') }} rd ON rd.RBUKRS = gt.RBUKRS
						 AND rd.RACCT = gt.RACCT

						 