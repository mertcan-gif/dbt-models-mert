{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

WITH unpivot_data AS (
    SELECT 
        [company],
        [business_area],
        [business_area_name],
        [date],
        data_control_date,
        [metric],
        CAST([value] AS FLOAT) AS value
    FROM (
        SELECT 
            [company],
            [business_area],
            [business_area_name],
            [date],
            data_control_date,
            CAST(ISNULL([final_updated_contract_value_try], -1) AS MONEY) AS final_updated_contract_value_try,
            CAST(ISNULL([advance_guarantee_letter_amount_try], -1) AS MONEY) AS advance_guarantee_letter_amount_try,
            CAST(ISNULL([contract_and_final_account_guarantee_amount_try], -1) AS MONEY) AS contract_and_final_account_guarantee_amount_try,
            CAST(ISNULL([price_difference_guarantee_amount_try], -1) AS MONEY) AS price_difference_guarantee_amount_try,
            CAST(ISNULL([advance_material_guarantee_amount_try], -1) AS MONEY) AS advance_material_guarantee_amount_try,
            CAST(ISNULL([final_guarantee_letter_amount_try], -1) AS MONEY) AS final_guarantee_letter_amount_try,
            CAST(ISNULL([invoiced_down_payment_progress_amount_try_with_ff], -1) AS MONEY) AS invoiced_down_payment_progress_amount_try_with_ff,
            CAST(ISNULL([invoiced_down_payment_progress_amount_try_excluding_ff], -1) AS MONEY) AS invoiced_down_payment_progress_amount_try_excluding_ff,
            CAST(ISNULL([employer_progress_payment_advancement_percentage], -1) AS MONEY) AS employer_progress_payment_advancement_percentage,
            CAST(ISNULL([end_of_job_collection_fee_try], -1) AS MONEY) AS end_of_job_collection_fee_try,
            CAST(ISNULL([realized_collection_try], -1) AS MONEY) AS realized_collection_try,
            CAST(ISNULL([total_progress_payment_incl_advance_eur], -1) AS MONEY) AS total_progress_payment_incl_advance_eur,
            CAST(ISNULL([total_progress_payment_excl_advance_eur], -1) AS MONEY) AS total_progress_payment_excl_advance_eur,
            CAST(ISNULL([epc_collection_amount_eur], -1) AS MONEY) AS epc_collection_amount_eur,
            CAST(ISNULL([spv_collection_amount_eur], -1) AS MONEY) AS spv_collection_amount_eur,
            CAST(ISNULL([epc_collection_progress_percentage], -1) AS MONEY) AS epc_collection_progress_percentage,
            CAST(ISNULL([manufacturing_progress_percentage], -1) AS MONEY) AS manufacturing_progress_percentage,
            CAST(ISNULL([spv_collection_progress_percentage], -1) AS MONEY) AS spv_collection_progress_percentage,
            CAST(ISNULL([collection_progress_percentage], -1) AS MONEY) AS collection_progress_percentage
        FROM 
            {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_employerprogresspayment') }}
    ) AS p
    UNPIVOT (
        [value] FOR [metric] IN (
            [final_updated_contract_value_try],
            [advance_guarantee_letter_amount_try],
            [contract_and_final_account_guarantee_amount_try],
            [price_difference_guarantee_amount_try],
            [advance_material_guarantee_amount_try],
            [final_guarantee_letter_amount_try],
            [invoiced_down_payment_progress_amount_try_with_ff],
            [invoiced_down_payment_progress_amount_try_excluding_ff],
            [employer_progress_payment_advancement_percentage],
            [end_of_job_collection_fee_try],
            [realized_collection_try],
            [manufacturing_progress_percentage],
            [total_progress_payment_incl_advance_eur],
            [total_progress_payment_excl_advance_eur],
            [epc_collection_amount_eur],
            [spv_collection_amount_eur],
            [epc_collection_progress_percentage],
            [spv_collection_progress_percentage],
            [collection_progress_percentage]
        )
    ) AS unpivoted_data
)

SELECT 
    rls_region = cm.RegionCode,
    rls_group = cm.KyribaGrup + '_' + cm.RegionCode,
    rls_company = ud.company + '_' + cm.RegionCode,
    rls_businessarea = TRIM(ud.business_area) + '_' + cm.RegionCode,
    [group] = cm.KyribaGrup,
    company,
    business_area,
    t.name1 AS businessarea_name,
    CAST(date AS date) AS date,
    CAST(data_control_date AS date) AS data_control_date,
    metric,
    value
FROM unpivot_data ud
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm ON TRIM(ud.company) = cm.KyribaKisaKod
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t ON TRIM(ud.business_area) = t.werks
