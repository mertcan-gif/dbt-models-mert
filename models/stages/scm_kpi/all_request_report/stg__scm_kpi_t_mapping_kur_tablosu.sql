{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}
    SELECT
        usd.date_value,
        usd.try_value AS usd_try_value,
        (
            SELECT eur.try_value
            FROM {{ ref('stg__dimensions_t_dim_dailys4currencies') }} eur
            WHERE eur.currency = 'EUR'
              AND eur.date_value = usd.date_value
        ) AS eur_try_value
    FROM {{ ref('stg__dimensions_t_dim_dailys4currencies') }} usd
    WHERE usd.currency = 'USD'