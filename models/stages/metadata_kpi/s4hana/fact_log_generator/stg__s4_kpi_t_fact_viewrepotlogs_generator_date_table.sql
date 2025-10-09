{{ config( materialized = 'table', tags = ['metadata_kpi'] ) }}

----- stg__s4_kpi_t_fact_viewrepotlogs_generator_filler_table'deki veride eksik olan tarihleri (0 gelen veya veri çekim hatasından ötürü boş gelen)
select _date = dt2.date

FROM (
	SELECT 
		dt.date,
		SUM(CAST([IslemSayisi] AS INT)) transactions
	FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} dt
		left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_reportusages') }} rpt ON dt.date = CAST([Tarih] AS DATE)
	where dt.date >= '2024-05-01' and dt.date < dateadd(day,-1,getdate())
	GROUP BY dt.date
	HAVING SUM(CAST([IslemSayisi] AS INT)) = 0 OR SUM(CAST([IslemSayisi] AS INT)) IS NULL
) dt2
