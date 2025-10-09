{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
/*
    Verilen avansların tutarları bulunmaktadır.
*/

WITH unique_project_and_subcontractor AS (
    /*
        Projelere göre filtrelenmesi için hakedişleri listelediğimiz tablodaki proje ve subcontractorlarla 
        filtrelenmesi için unique_project_and_subcontractor oluşturulmuştur.
    */
    SELECT DISTINCT
        company 
        ,project_code
        ,subcontractor_no
    FROM {{ ref('stg__to_kpi_t_fact_progresspaymentsap') }}
    )
SELECT 
    acd.rbukrs AS company
    ,acd.rbusa AS business_area
    ,t001w.name1 AS businessarea_description
    ,acd.lifnr AS subcontractor_no
    ,lfa1.name1 AS subcontractor_name
    ,SUM(CAST(acd.fcsl as money)) advance_amount_tl
    ,acd.gjahr AS fiscal_year
    ,CASE
        WHEN acd.umskz IN ('A', 'N', 'I', 'Z') THEN 1
        ELSE 0
    END AS advance_flag
    ,CAST(acd.bldat AS date)AS document_date
FROM {{ ref('stg__s4hana_t_sap_acdoca') }} acd
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w on acd.rbusa = t001w.werks
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa1 on lfa1.lifnr = acd.lifnr
LEFT JOIN unique_project_and_subcontractor spp on spp.company = acd.rbukrs
                                                AND spp.project_code = acd.rbusa
                                                AND spp.subcontractor_no = acd.lifnr
WHERE acd.umskz IN ('A', 'N', 'I', 'Z', 'B')
    AND acd.blart = 'S1'
    AND acd.drcrk = 'S'
    AND acd.koart = 'K'
    AND spp.company IS NOT NULL
GROUP BY
    acd.rbukrs
    ,acd.rbusa
    ,acd.gjahr
    ,acd.lifnr
    ,t001w.name1
    ,acd.umskz
    ,lfa1.name1
    ,CAST(acd.bldat AS date)