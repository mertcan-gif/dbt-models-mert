{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}
select
c.*,
l.lifnr as vendor_code
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfi_001_t_casers') }} c
LEFT JOIN 
    (
    SELECT DISTINCT
    STCD2,
    lifnr
    from
    {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} ) l on l.STCD2 = c.stcd2
WHERE 1=1
      and c.stcd2 is not null
      and c.stcd2 != ''

