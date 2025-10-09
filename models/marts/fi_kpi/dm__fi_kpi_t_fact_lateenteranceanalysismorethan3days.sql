{{
  config(
    materialized = 'table',tags = ['fi_kpi']
    )
}}

/*
	BLART kolonu UE olmayanlar için filtrelendiğinde SK olan belge türleri de gelmektedir.
	Ancak diğer tarif olan BLART'I S1, S2 ve S3 olan değerleri aldığımızda SK belge türleri gelmeyeceği için arada fark oluşturmaktadır.
*/

with cte as (
	SELECT
		ACDOCA.RBUKRS --Şirket Kodu
		,ACDOCA.BELNR
		,ACDOCA.BUZEI
		,ACDOCA.GJAHR
		,LEFT(CAST(ACDOCA.BLDAT AS DATE), 7) AS YEARMONTH
		,cast(acdoca.bldat as date) bldat
		,cast(bkpf.cpudt as date)record
		, DATEDIFF(DAY, CAST(acdoca.bldat as date), CAST(BKPF.CPUDT AS DATE)) AS total_day
		, (DATEDIFF(DAY, CAST(acdoca.bldat as date), CAST(BKPF.CPUDT AS DATE)) -- Total days
			- (DATEDIFF(WEEK, CAST(acdoca.bldat as date), CAST(BKPF.CPUDT AS DATE)) * 2) -- Subtract weekends
			- CASE WHEN DATEPART(WEEKDAY, CAST(acdoca.bldat as date)) = 1 THEN 1 ELSE 0 END -- Adjust if start date is Sunday
			- CASE WHEN DATEPART(WEEKDAY, CAST(BKPF.CPUDT AS DATE)) = 7 THEN 1 ELSE 0 END -- Adjust if end date is Saturday
		) AS diff_with_working_date
		, ABS(KSL) AS ABS_KSL
	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} AS ACDOCA
		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} BKPF ON ACDOCA.BELNR = BKPF.BELNR
										AND ACDOCA.RBUKRS = BKPF.BUKRS
										AND ACDOCA.GJAHR = BKPF.GJAHR
	WHERE 1=1
		AND LEFT(RACCT,3) IN ('102')
		-- AND ACDOCA.BLART <> 'UE'
		AND ACDOCA.BLART IN ('S1', 'S2', 'S3')
		AND bldat >= '20241201'
		AND bkpf.xreversing = 0
		AND bkpf.xreversed = 0 
	)
,new_date_diff AS (
	select 
		cte.*
		,CASE	
			WHEN diff_with_working_date < 0 THEN 0.00
			ELSE diff_with_working_date
		END AS BLDAT_CPUDT_DIFF
	from cte
	)
,raw_cte AS (
SELECT 
	*
	,CASE
		WHEN BLDAT_CPUDT_DIFF > 3 THEN 1
		ELSE 0
	END AS late_record_flag
	,BLDAT_CPUDT_DIFF * ABS_KSL AS DATE_DIFF_MULT_ABS_KSL
FROM new_date_diff
	)

,add_total_volume AS (
	SELECT 
		RBUKRS,
		YEARMONTH,
		BLDAT_CPUDT_DIFF,
		late_record_flag,
		ABS_KSL,
		DATE_DIFF_MULT_ABS_KSL,
		COUNT(CASE WHEN late_record_flag = 1 THEN 1 END) OVER (PARTITION BY RBUKRS, yearmonth) AS late_record_count,
		COUNT(*) OVER (PARTITION BY RBUKRS,yearmonth) AS total_record,
		SUM(ABS_KSL) OVER (PARTITION BY RBUKRS,yearmonth) total_volume,
		SUM(CASE WHEN late_record_flag = 1 THEN ABS_KSL ELSE 0 END) OVER (PARTITION BY RBUKRS, yearmonth) AS total_late_volume
	FROM RAW_CTE
	WHERE 1=1
	)

,final AS (
	SELECT 
		*
		,late_record_ratio = late_record_count * 1.0 / total_record * 1.0
		,CASE
			WHEN total_volume = 0 THEN NULL
			ELSE total_late_volume / total_volume
		END AS late_volume_ratio
		,CASE
			WHEN total_late_volume = 0 THEN NULL
			ELSE DATE_DIFF_MULT_ABS_KSL * 1.0  / total_late_volume  * 1.0
		END AS weighted_average_based_on_late_records
		,CASE
			WHEN total_volume = 0 THEN NULL
			ELSE DATE_DIFF_MULT_ABS_KSL * 1.0 / total_volume * 1.0
		END AS weighted_average_based_on_total_transaction_volume
	FROM add_total_volume
	)

SELECT
	rls_region = cm.RegionCode
	,rls_group = CONCAT(cm.KyribaGrup, '_', cm.RegionCode)
	,rls_company = CONCAT(f.rbukrs, '_', cm.RegionCode)
	,rls_businessarea = CONCAT('_',cm.RegionCode)
	,f.rbukrs AS company
	,yearmonth
	,late_record_count
	,total_late_volume
	,CAST(AVG(ISNULL(BLDAT_CPUDT_DIFF, 0)) AS DECIMAL(18,2)) average_record_day
	,COALESCE(CAST(AVG(CASE WHEN late_record_flag = 1 THEN ISNULL(BLDAT_CPUDT_DIFF, 0) END) AS DECIMAL(18,2)) ,0) average_late_record_day
	,CAST(AVG(ISNULL(late_record_ratio, 0)) AS DECIMAL(18,2)) late_record_ratio
	,CAST(AVG(ISNULL(late_volume_ratio, 0)) AS DECIMAL(18,2)) late_transaction_volume_ratio
	,COALESCE(CAST(SUM(ISNULL(weighted_average_based_on_late_records, 0)) AS DECIMAL(18,2)) ,0) weighted_average_based_on_late_record
	,COALESCE(CAST(SUM(ISNULL(weighted_average_based_on_total_transaction_volume, 0)) AS DECIMAL(18,2)) ,0) weighted_average_based_on_total_transaction_volume
FROM final f
LEFT JOIN {{ ref("dm__dimensions_t_dim_companies") }} cm on cm.RobiKisaKod = rbukrs
GROUP BY rbukrs
		,yearmonth
		,cm.RegionCode
		,cm.KyribaGrup
		,f.late_record_count
		,f.total_late_volume
