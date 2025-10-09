{{
  config(
    materialized = 'table',tags = ['hr_kpi','rmore']
    )
}}

/* 
Date: 20250909
Creator: Elif Erdal
Report Owner: Lale Taşdelen - HR Systems
Explanation: RFlow uzerindeki tablet talepleri ve onaycilarinin yorumlari istenmistir. Bu amacla RMore'da yayinlanmak uzere hazirlanmistir.
*/


SELECT 
    rls_region = rls.[rls_region]
    ,rls_group = rls.[rls_group]
    ,rls_company = rls.[rls_company]
    ,rls_businessarea = rls.[rls_businessarea]
    ,rls_key=CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
    ,r5.[surec_no] as [process_number]
    ,r5.[islem_id] as [process_id]
    ,r3.[sicil] as [sap_id]
    ,emp.[ronesans_rank_personal_tr] as [ronesans_rank_personal]
    ,r3.[olusturan] as [creator_full_name]
    ,r3.adsoyad as [on_behalf_of]
    ,r3.[malzemealttip] as [material_sub_type]
    ,r3.[durum] as [creator_status]
    ,r5.[adsoyad] as [approval_full_name]
    ,r5.gorev_adi as [status_role]
    ,r5.[statu] as [approval_status]
    ,r5.[yorum] as [comment]
    ,case when olusturansicil!=globalsicil then 1
    when yeni_personel='X' then 1
    else 0 end created_on_behalf_flag
    ,CAST(CONCAT(r5.[tarih], ' ', [saat]) AS DATETIME) AS [date]
    ,alvl.[name_tr] as [group]
	  ,blvl.[name_tr] as [company]
	  ,clvl.[name_tr] as [region]
	  ,emp.[payroll_company]
	  ,emp.[unit] as [department]
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zmtf_t_03') }} r3
  INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_rflow_zmtf_t_05') }} r5
    ON r3.[surec_no] = r5.[surec_no]
  LEFT JOIN {{ ref('stg__hr_kpi_t_dim_sf_rls') }} as rls
    ON rls.[sap_id] = r3.[sicil]
  LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} emp
    ON emp.user_id = r3.sicil
  LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_a') }} alvl
    ON alvl.code = emp.a_level_code
  LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_b') }} blvl
    ON blvl.code = emp.b_level_code
  LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_c') }} clvl
    ON clvl.code = emp.c_level_code
  WHERE r3.[malzemealttip] = 'Tablet'
