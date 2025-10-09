{{ config( materialized = 'table', tags = ['metadata_kpi'] ) }}

----- (real_table) ilk önce geçmiş verisinde eksiklikleri olan tabloyu çekiyorum  AÇIKLAMA 1 (DEVAMI İÇİN AÇIKLAMA 2'Yİ OKUYUNUZ)
----- (filler_table) Daha sonra tüm verileri içeren ancak sadece transaction detayında içeren tabloyu çekiyorum AÇIKLAMA 2 (DEVAMI İÇİN AÇIKLAMA 3'ü OKUYUNUZ)
----- (date_table)Açıklama 1'deki veride eksik olan tarihleri (0 gelen veya veri çekim hatasından ötürü boş gelen) AÇIKLAMA 3 (DEVAMI İÇİN AÇIKLAMA 4'ü OKUYUNUZ)


with cte as (
    SELECT *
    FROM {{ ref('stg__s4_kpi_t_fact_viewrepotlogs_generator_real_table') }}
    UNION ALL 
    select *
    from {{ ref('stg__s4_kpi_t_fact_viewrepotlogs_generator_filler_table') }} 
    where creation_time	 IN (select * from {{ ref('stg__s4_kpi_t_fact_viewrepotlogs_generator_date_table') }})
)

select
cte.*,
rp.[name] AS report_name
from cte
LEFT JOIN {{ ref('stg__s4_kpi_t_dim_reports') }} rp ON cte.report_id = rp.id
where creation_time >= '2024-05-01'