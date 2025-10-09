{{
  config(
    materialized = 'table',tags = ['rlsdimensions']
    )
}}

-- TODO: AYNI BUSINESSAREACODE'LARE TEKRAR ETMEYECEK ŞEKİLDE NON-SAP BUSINESSAREA'LAR DA EKLENMELİ

with raw_data as (
	Select 
		rbukrs,
		rbusa,
		COUNT(*) document_count
	from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_acdoca_buzei') }}
	where rbusa <> ''
	GROUP BY rbukrs,rbusa
)
, raw_data_2 AS (
	SELECT 
		*,
		ROW_NUMBER() 
		OVER(
			PARTITION BY 
			rbusa
			ORDER BY document_count DESC
		) dups_flag
	FROM raw_data
	)

SELECT 
	   [rls_region] = dim_cmp.RegionCode
      ,[rls_group] = CONCAT(dim_cmp.KyribaGrup,'_',dim_cmp.RegionCode)
      ,[rls_company] = CONCAT(dim_cmp.RobiKisaKod,'_',dim_cmp.RegionCode)
      ,[rls_businessarea] = CONCAT(raw_data_2.rbusa,'_',dim_cmp.RegionCode)
      ,[group_code] = dim_cmp.KyribaGrup
      ,[company_code] = rbukrs
      ,[businessarea_code] = rbusa
      ,[project_name] = UPPER(tgsbt.gtext)
FROM raw_data_2
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }}  dim_cmp ON dim_cmp.RobiKisaKod = raw_data_2.rbukrs
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tgsbt') }}  tgsbt on raw_data_2.rbusa = tgsbt.gsber
WHERE dups_flag = 1 AND tgsbt.SPRAS = 'TR'