{{ config( materialized = 'table', tags = ['metadata_kpi'] ) }}


WITH report_names AS
(

SELECT * FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tstct') }} WHERE SPRSL  = 'T'
)

SELECT DISTINCT 
	IslemAdi AS [id],
	'S4HANA' as [report_type],
	COALESCE(rn.ttext, ru.IslemAdi) as [name],
	'1900-01-01' as [created_date_time],
	'1900-01-01' as [modified_date_time],
	'S4HANA' as [modified_by],
	'S4HANA' as [created_by],
	'S4HANA' as [workspace_id],
	'S4HANA' as [sub_segment],
	'S4HANA' as [segment]
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_reportusages') }} ru
	LEFT JOIN report_names rn ON rn.tcode = ru.IslemAdi
