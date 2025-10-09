{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
WITH final_cte as (

    -- ACCOMMODATION COST START --

    SELECT
            'accommodation' as process_type,
            LTRIM(RTRIM(SGF)) as code,
            SUM(TRY_CAST([TOPLAM TUTAR] AS float)) as raw_cost,
            NULL as bonus,
            [Fatura No] as invoice_number,
            NULL as transport_invoice_date,
            TRY_CAST([FATURA KESİM TARİHİ] AS date) AS accom_invoice_date,
            NULL as refund,
            SUM(TRY_CAST([TOPLAM TUTAR] AS float)) as cost,
            NULL as penalty_cost,
            '' as tip
    FROM  {{ source('stg_sharepoint', 'raw__rmh_kpi_t_fact_accommodationcosts') }}
    WHERE 1=1
            AND LTRIM(RTRIM(SGF)) IS NOT NULL
            AND LTRIM(RTRIM(SGF)) LIKE 'SGF-%'
            AND CHARINDEX('/', SGF) = 0
            and LEN(LTRIM(RTRIM(TRY_CAST([FATURA KESİM TARİHİ] AS date)))) >0
    GROUP BY LTRIM(RTRIM(SGF)),TRY_CAST([FATURA KESİM TARİHİ] AS date),[Fatura No]

    -- ACCOMMODATION COST END --



    UNION ALL


    -- TRANSPORTATION (FLIGHTS) COST START --

    SELECT
            'transportation' as process_type,
            LTRIM(RTRIM(SGF)) as code,
            SUM(TRY_CAST([TOPLAM TUTAR] AS float)) as raw_cost,
            case
                when  [İÇ HAT-DIŞ HAT] = 'I' THEN 6
                when  [İÇ HAT-DIŞ HAT] = 'D' THEN 32
                else NULL
            end as bonus,
            r.[FATURA NUMARASI] as invoice_number,
            TRY_CAST([FATURA KESİM TARİHİ] AS DATE)  as transport_invoice_date,
            NULL as accom_invoice_date,
            t.[İADE TUTARI] as refund,
            CASE
                WHEN CAST(r.[FATURA NUMARASI] AS VARCHAR(255)) = CAST(t.[FATURA NUMARASI] AS VARCHAR(255))
                    AND r.[FATURA NUMARASI] IS NOT NULL
                THEN 0
                ELSE SUM(TRY_CAST(REPLACE(r.[TOPLAM TUTAR], ',', '') AS FLOAT))  -- Remove commas if present
            END AS cost,
            CASE
                WHEN CAST(r.[FATURA NUMARASI] AS VARCHAR(255)) = CAST(t.[FATURA NUMARASI] AS VARCHAR(255))
                    AND r.[BİLET NO] = t.[BİLET NUMARASI]
                    AND r.[FATURA NUMARASI]  IS NULL  
                THEN 0
                ELSE TRY_CAST(REPLACE(r.[TOPLAM TUTAR], ',', '') AS FLOAT)  - ABS(TRY_CAST(REPLACE(t.[İADE TUTARI], ',', '') AS FLOAT))
            END AS penalty_cost,
            '' as tip
    FROM {{ source('stg_sharepoint', 'raw__rmh_kpi_t_fact_transportationcosts') }} r
        LEFT JOIN {{ source('stg_sharepoint','raw__rmh_kpi_t_fact_transportationrefunds') }} as t ON
                                            CAST(r.[FATURA NUMARASI] AS VARCHAR(255)) = CAST(t.[FATURA NUMARASI] AS VARCHAR(255))
                                            and  CAST(r.[SGF] AS VARCHAR(255)) = CAST(t.[SGF NUMARASI] AS VARCHAR(255))
                                            AND r.[BİLET NO] = t.[BİLET NUMARASI]
    WHERE 1=1
        AND LTRIM(RTRIM(SGF)) IS NOT NULL
        AND LTRIM(RTRIM(SGF)) LIKE 'SGF-%'
        and LEN(LTRIM(RTRIM(TRY_CAST([FATURA KESİM TARİHİ] AS date)))) >0
    GROUP BY LTRIM(RTRIM(SGF)),[İÇ HAT-DIŞ HAT],r.[FATURA NUMARASI],t.[FATURA NUMARASI],TRY_CAST([FATURA KESİM TARİHİ] AS DATE),t.[İADE TUTARI], r.[TOPLAM TUTAR],t.[BİLET NUMARASI],r.[BİLET NO]


    -- TRANSPORTATION (FLIGHTS) COST END --

    UNION ALL
    

    -- TRANSPORTATION (BUS) COST START --

    SELECT
            'transportation' as process_type,
            LTRIM(RTRIM(SGF)) as code,
            SUM(TRY_CAST([Bilet Fatura Toplam] AS float)) as raw_cost,
            6 as bonus,
            r.[Fatura No] as invoice_number,
            TRY_CAST(r.[Fatura Kesim Tarihi] AS date) as transport_invoice_date,
            NULL as accom_invoice_date,
            SUM(t.[İADE TUTARI]) as refund,
            CASE
                WHEN CAST(r.[Fatura No] AS VARCHAR(255)) = CAST(t.[FATURA NUMARASI] AS VARCHAR(255))
                    AND r.[Fatura No] IS NOT NULL
                THEN 0
                ELSE SUM(TRY_CAST(REPLACE(r.[Bilet Fatura Toplam], ',', '') AS FLOAT))  -- Remove commas if present
            END AS cost,
            CASE
                WHEN CAST(r.[Fatura No] AS VARCHAR(255)) = CAST(t.[FATURA NUMARASI] AS VARCHAR(255))
                    AND r.[Fatura No] IS
                    NULL
                THEN 0
                ELSE TRY_CAST(REPLACE(r.[Bilet Fatura Toplam], ',', '') AS FLOAT) - ABS(TRY_CAST(REPLACE(t.[İADE TUTARI], ',', '') AS FLOAT))
            END AS penalty_cost,
            '' as tip
    FROM {{ source('stg_sharepoint','raw__rmh_kpi_t_fact_bustransportationcosts') }} r
        LEFT JOIN {{ source('stg_sharepoint','raw__rmh_kpi_t_fact_transportationrefunds') }} as t ON
                                            CAST(r.[Fatura No] AS VARCHAR(255)) = CAST(t.[FATURA NUMARASI] AS VARCHAR(255))
                                            and CAST(r.[SGF] AS VARCHAR(255)) = CAST(t.[SGF NUMARASI] AS VARCHAR(255))
    WHERE 1=1                                    
        AND LTRIM(RTRIM(SGF)) IS NOT NULL
        AND LTRIM(RTRIM(SGF)) LIKE 'SGF-%'
        and LEN(LTRIM(RTRIM(TRY_CAST([Fatura Kesim Tarihi] AS date)))) >0
    GROUP BY LTRIM(RTRIM(SGF)),r.[Fatura No],t.[FATURA NUMARASI],TRY_CAST(r.[Fatura Kesim Tarihi] AS date),[İADE TUTARI],[Bilet Fatura Toplam]
    
    -- TRANSPORTATION (BUS) COST END --

    UNION ALL
    
    -- TRANSPORTATION (TRAIN) COST START --

    SELECT
            'transportation' as process_type,
            LTRIM(RTRIM(SGF)) as code,
            SUM(TRY_CAST([Bilet Fatura Toplam] AS float)) as raw_cost,
            6 as bonus,
            r.[Fatura No] as invoice_number,
            TRY_CAST(r.[Fatura Kesim Tarihi] AS date) as transport_invoice_date,
            NULL as accom_invoice_date,
            SUM(t.[İADE TUTARI]) as refund,
            CASE
                WHEN CAST(r.[Fatura No] AS VARCHAR(255)) = CAST(t.[FATURA NUMARASI] AS VARCHAR(255))
                    AND r.[Fatura No] IS NOT NULL
                THEN 0
                ELSE SUM(TRY_CAST(REPLACE(r.[Bilet Fatura Toplam], ',', '') AS FLOAT))  -- Remove commas if present
            END AS cost,
            CASE
                WHEN CAST(r.[Fatura No] AS VARCHAR(255)) = CAST(t.[FATURA NUMARASI] AS VARCHAR(255))
                    AND r.[Fatura No] IS
                    NULL
                THEN 0
                ELSE TRY_CAST(REPLACE(r.[Bilet Fatura Toplam], ',', '') AS FLOAT)- ABS(TRY_CAST(REPLACE(t.[İADE TUTARI], ',', '') AS FLOAT))
            END AS penalty_cost,
            '' as tip
    FROM {{ source('stg_sharepoint','raw__rmh_kpi_t_fact_traintransportationcosts') }} r
    LEFT JOIN {{ source('stg_sharepoint','raw__rmh_kpi_t_fact_transportationrefunds') }} as t ON
                                        CAST(r.[Fatura No] AS VARCHAR(255)) = CAST(t.[FATURA NUMARASI] AS VARCHAR(255))
                                        AND CAST(r.[SGF] AS VARCHAR(255)) = CAST(t.[SGF NUMARASI] AS VARCHAR(255))
    WHERE 1=1
        AND LTRIM(RTRIM(SGF)) IS NOT NULL
        AND LTRIM(RTRIM(SGF)) LIKE 'SGF-%'
        AND LEN(LTRIM(RTRIM(TRY_CAST([Fatura Kesim Tarihi]  AS date)))) >0
    GROUP BY LTRIM(RTRIM(SGF)),r.[Fatura No],t.[FATURA NUMARASI],TRY_CAST(r.[Fatura Kesim Tarihi] AS date),[İADE TUTARI],[Bilet Fatura Toplam]

    -- TRANSPORTATION (TRAIN) COST END --


UNION ALL
    
    -- TRANSPORTATION (corporate vehicle) COST START --

    SELECT
            'transportation' as process_type,
            LTRIM(RTRIM(SGF)) as code,
            SUM(TRY_CAST([Tutar] AS float)) as raw_cost,
            0 as bonus,
            r.[Fatura No] as invoice_number,
            TRY_CAST(r.[Fatura Kesim Tarihi] AS date) as transport_invoice_date,
            NULL as accom_invoice_date,
            0 as refund,
            SUM(TRY_CAST([Tutar] AS float)) cost,
            0 penalty_cost,
            'vehicle' as tip
    FROM {{ source('stg_sharepoint','raw__rmh_kpi_t_fact_transportationcorporatevehicle') }} r
    WHERE 1=1
        AND LTRIM(RTRIM(SGF)) IS NOT NULL
        AND LTRIM(RTRIM(SGF)) LIKE 'SGF-%'
        and [Tutar] is not null
        and [Tutar] != ''
    GROUP BY LTRIM(RTRIM(SGF)),r.[Fatura No],TRY_CAST(r.[Fatura Kesim Tarihi] AS date)

    -- TRANSPORTATION (TRAIN) COST END --

    UNION ALL

    -- AIRPORT TRANSFER COST START --

    SELECT
            a.process_type,
            a.code,
            SUM(a.cost) AS cost ,
            NULL as bonus,
            NULL as invoice_number,
            NULL AS transport_invoice_date,
            NULL as accom_invoice_date,
            NULL as refund,
            SUM(a.cost) AS cost,
            NULL as penalty_cost,
            '' as tip
    FROM
            (SELECT
                'transfer' AS process_type,
                LTRIM(RTRIM([SGF NO])) AS code,
                TRY_CAST([ÜCRET(KDVLİ)] AS float) AS cost
            FROM {{ source('stg_sharepoint', 'raw__rmh_kpi_t_fact_ankaraairporttransfercosts') }}
            WHERE 1=1
                AND LTRIM(RTRIM([SGF NO])) IS NOT NULL
                AND LTRIM(RTRIM([SGF NO])) LIKE 'SGF-%'
            UNION ALL
            SELECT
                'transfer' AS process_type,
                LTRIM(RTRIM([SGF])) AS code,
                TRY_CAST([Araç Fiyat] AS float) AS cost
            FROM {{ source('stg_sharepoint', 'raw__rmh_kpi_t_fact_istanbulairporttransfercosts') }}
            WHERE 1=1
                AND LTRIM(RTRIM([SGF])) IS NOT NULL
                AND LTRIM(RTRIM([SGF])) LIKE 'SGF-%') a 
    GROUP BY a.code,a.process_type

-- AIRPORT TRANSFER COST END --

)
select * 
from final_cte
where 1=1
    and (transport_invoice_date is not null or accom_invoice_date is not null or tip = 'vehicle')
    or process_type = 'transfer'
