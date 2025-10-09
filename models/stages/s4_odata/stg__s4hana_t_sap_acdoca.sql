{{
  config(
    materialized = 'table',tags = ['s4_odata','acdoca_full'],
    post_hook = [
      "CREATE NONCLUSTERED INDEX idx_rbukrs ON {{ this }} (rbukrs)",
      "CREATE NONCLUSTERED INDEX idx_gjahr ON {{ this }} (gjahr)",
      "CREATE NONCLUSTERED INDEX idx_belnr ON {{ this }} (belnr)",
      "CREATE NONCLUSTERED INDEX idx_bldat ON {{ this }} (bldat)",
      "CREATE NONCLUSTERED INDEX idx_budat ON {{ this }} (budat)"
    ]
    )
}}


SELECT
    *
from {{ ref('stg__s4hana_t_sap_acdoca_full') }}
where 1=1
    and vorgn in ('AS91','AZAF','AZBU','AZUM','GLYC','HRP1','RFBU',
                'RFST','RMBL','RMRP','RMWA','RMWE','RMWL','SD00',
                'UMAI','UMZI','ZUGA','ABGA','ACEA')
