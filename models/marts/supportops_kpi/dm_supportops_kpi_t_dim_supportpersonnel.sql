{{
  config(
    materialized = 'table',tags = ['supportops_kpi']
    )
}}	

/*
Filtre olarak verilen firmalar Tuğba Hanım'ın yönlendirilmesiyle eklenmiştir.
    Satıcı	Satıcı
    1009543	KSK TEMİZLİK TURİZM VE ORGANİZASYON
    1014157	ERAL TEMİZLİK LOJİSTİK İNŞAAT
    1031761	MCK İÇ VE DIŞ TİCARET YAPI İNŞAAT S
    1002027	DOMİNO ENTEGRE HİZMET YÖNETİMİ LOJİ
    1034132	SET ENTEGRE HİZMET YÖNETİMİ LOJİSTİ
    1038038	REAL DESTEK HİZMETLERİ İNŞ. TİC.LTD
*/

SELECT
    sp.[rls_region]
    ,sp.[rls_company]
    ,sp.[rls_businessarea]
    ,sp.[rls_group]
    --,sp.[rls_key]
    ,sp.[sap_id]
    --,sp.[transaction_date]
    ,sp.[start_date]
    ,sp.[full_name]
    ,CASE 
        WHEN transaction_distribution = N'İşten ayrılma' THEN sp.[start_date]
        WHEN sp.[end_date] = '9999-12-31' AND transaction_distribution <> N'İşten ayrılma' THEN CAST(GETDATE() AS DATE)
        ELSE sp.[end_date]
    END AS end_date
    ,sp.[transaction_distribution]
    ,sp.[position]
    ,sp.[statu]
    ,sp.[direct_indirect]
    ,sp.[blue_white_collar]
    ,sp.[project]
    ,sp.[year_of_seniority]
    ,sp.[gender]
    ,sp.[production_class]
    ,sp.[age]
    ,sp.[leaving_work]
    ,sp.[nationality]
    ,sp.[team_code]
    ,sp.[team_based] as team_description
    ,ekp.approver1 AS manager_id
    ,CONCAT(emp.name, ' ', emp.surname) AS manager_fullname
    ,sp.[team_based]
    ,sp.[country]
    ,sp.[transportation]
    ,sp.[accommodation]
    ,sp.[education]
    ,sp.[company_class]
    ,sp.[main_discipline]
    ,sp.[sub_subcontractor]
    ,sp.[subcontractor]
    ,sp.[employee_group]
    ,sp.[personnel_subfield]
    ,sp.[reason_for_termination_code]
    ,sp.[leaving_reason]
    ,sp.[task_type]
    ,sp.[location]
    ,sp.[business_area]
    ,sp.[group]
    ,sp.[company]
    ,sp.[name]
    ,sp.[scale]
    ,sp.[scale_description]
FROM {{ ref('dm__hr_kpi_t_dim_subcontractorpersonnel') }} sp
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa on lfa.name1 = sp.sub_subcontractor
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zhr_ekip_krilim2') }} ekp on ekp.werks = sp.company
                                                                                and ekp.btrtl = sp.personnel_subfield
                                                                                and ekp.ekipno = sp.team_code
                                                                                and (sp.start_date >= CAST(ekp.begda as date) AND sp.end_date <= CAST(ekp.endda as date))
LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} emp on emp.sap_id = ekp.approver1
where 1=1
	and sp.sub_subcontractor IN (
						N'KSK TEMİZLİK TURİZM VE ORGANİZASYON',
						N'ERAL TEMİZLİK LOJİSTİK İNŞAAT',
						N'MCK İÇ VE DIŞ TİCARET YAPI İNŞAAT S',
						N'DOMİNO ENTEGRE HİZMET YÖNETİMİ LOJİ',
						N'SET ENTEGRE HİZMET YÖNETİMİ LOJİSTİ',
                        N'REAL DESTEK HİZMETLERİ İNŞ. TİC.LTD'
					)
