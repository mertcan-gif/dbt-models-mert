{{
  config(
    materialized = 'table',tags = ['hr_kpi_puantaj']
    )
}}

/* 
2025-02-25 ANK: Dimension tablosunda olan ancak hiç puantaj verisi gelmeyen çalışanlar listelenmek istenmektedir. Bu amaçla tablo hazırlanmıştır. 
*/

select 
rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group),
*
from {{ ref('dm__hr_kpi_t_dim_subcontractorpersonnel') }}
    where sap_id not in 
    (
        select distinct sap_id from {{ ref('dm__hr_kpi_t_fact_subcontractorpersonnellogs') }}
    )
    and sap_id not like '0000%'