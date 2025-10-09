{{
  config(
    materialized = 'view',tags = ['fi_kpi']
    )
}}

SELECT t001.bukrs,
       t001.butxt,
       t001.ort01,
       t001.land1,
       t001.waers,
       t001.spras
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001') }} AS t001 WITH (NOLOCK)
	LEFT OUTER JOIN {{ source('stg_fi_kpi', 'raw__fi_kpi_t_dim_closedcompanies') }} closed_companies WITH (NOLOCK) ON t001.BUKRS = closed_companies.BUKRS
WHERE (t001.FIKRS = 'RONS' OR t001.FIKRS = 'RTOR')
  AND (closed_companies.BUKRS IS NULL)
  AND t001.BUKRS NOT IN ('RHR','REV','RPS') 

