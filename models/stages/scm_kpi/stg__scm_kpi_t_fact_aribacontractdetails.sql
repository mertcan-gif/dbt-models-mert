
{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}

SELECT 
    -- Main pivot for textValue
    main.cowid,
    main.[Company Code], 
    main.[Document Category],
    main.[Document Type],
    main.[Payment Terms],
    main.[Purchasing Group],
    main.[Purchasing Organization],
    main.[Insurer],
    main.[Risk Address (Location)],
    main.[Smart Code],

    -- Pivot for flexMasterDataType
    o.[Contract Type],
    o.[Contract Group / Contract Unit],
    o.[Document Kind],
    o.[Plant],

    -- Pivot for moneyValue (main values like Contract Amount, Performance Bond, Advance Payment)
    h.[Contract Amount 2],
    z.[Contract Amount 2] as 'Contract Amount 2 Currency',
    h.[Performance Bond Amount 1],
    z.[Performance Bond Amount 1] as 'Performance Bond Amount 1 Currency',
    h.[Performance Bond Amount 2],
    z.[Performance Bond Amount 2] as 'Performance Bond Amount 2 Currency',
    h.[Advance Payment Amount 1],
    z.[Advance Payment Amount 1] as 'Advance Bond Amount 1 Currency',
    h.[Advance Payment Amount 2],
    z.[Advance Payment Amount 2] as 'Advance Bond Amount 2 Currency',

    -- Pivot for int_value
    j.[Personnel Limit Number],
    j.[Performance Bond Period (Month)],
    j.[Amendment - Phase],
    j.[Amendment - Count],
    j.[Duration of Work (Days)],
    j.[Advance Payment Bond Period (Month)],

    -- Pivot for percentage
    t.[Performance Bond Rate (%)],
    t.[Advance Payment Deduction Rate (%)],
    t.[Advance Payment Rate (%)],

    -- Pivot for datevalue
    k.[Unit Price Validity Date]

FROM 
    -- Main textValue pivot
    (SELECT DISTINCT
        cowid, 
        custom_field_name, 
        textValue
    FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_301') }}) AS SourceTable
    PIVOT 
    (
        MAX(textValue)
        FOR custom_field_name IN ([Company Code],[Document Category],[Document Type],[Payment Terms],[Purchasing Group],[Purchasing Organization],[Insurer],[Risk Address (Location)],[Smart Code])
    ) AS main

-- Join with flexMasterDataType pivot
LEFT JOIN 
    (SELECT DISTINCT
        cowid, 
        custom_field_name, 
        flexMasterDataType
    FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_301') }}) AS SourceTable
    PIVOT 
    (
        MAX(flexMasterDataType)
        FOR custom_field_name IN ([Contract Type],[Contract Group / Contract Unit],[Document Kind],[Plant])
    ) AS o ON o.cowid = main.cowid

-- Join with moneyValue pivot
LEFT JOIN 
    (SELECT DISTINCT
        cowid, 
        custom_field_name, 
        moneyValue
    FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_301') }}) AS SourceTable
    PIVOT 
    (
        MAX(moneyValue)
        FOR custom_field_name IN ([Contract Amount 2],[Performance Bond Amount 1],[Advance Payment Amount 2],[Advance Payment Amount 1],[Performance Bond Amount 2])
    ) AS h ON h.cowid = main.cowid

-- Join with moneyValue_currency pivot
LEFT JOIN 
    (SELECT DISTINCT
        cowid, 
        custom_field_name, 
        moneyValue_currency
    FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_301') }}) AS SourceTable
    PIVOT 
    (
        MAX(moneyValue_currency)
        FOR custom_field_name IN ([Contract Amount 2],[Performance Bond Amount 1],[Advance Payment Amount 2],[Advance Payment Amount 1],[Performance Bond Amount 2])
    ) AS z ON z.cowid = main.cowid

-- Join with int_value pivot
LEFT JOIN 
    (SELECT DISTINCT
        cowid, 
        custom_field_name, 
        int_value
    FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_301') }}) AS SourceTable
    PIVOT 
    (
        MAX(int_value)
        FOR custom_field_name IN ([Personnel Limit Number],[Performance Bond Period (Month)],[Amendment - Phase],[Amendment - Count],[Duration of Work (Days)],[Advance Payment Bond Period (Month)])
    ) AS j ON j.cowid = main.cowid

-- Join with percentage pivot
LEFT JOIN 
    (SELECT DISTINCT
        cowid, 
        custom_field_name, 
        percentage
    FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_301') }}) AS SourceTable
    PIVOT 
    (
        MAX(percentage)
        FOR custom_field_name IN ([Performance Bond Rate (%)],[Advance Payment Deduction Rate (%)],[Advance Payment Rate (%)] )
    ) AS t ON t.cowid = main.cowid

-- Join with datevalue pivot
LEFT JOIN 
    (SELECT DISTINCT
        cowid, 
        custom_field_name, 
        datevalue
    FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_301') }}) AS SourceTable
    PIVOT 
    (
        MAX(datevalue)
        FOR custom_field_name IN ([Unit Price Validity Date])
    ) AS k ON k.cowid = main.cowid;

