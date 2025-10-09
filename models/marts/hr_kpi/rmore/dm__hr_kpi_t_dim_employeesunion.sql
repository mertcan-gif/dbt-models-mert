SELECT
    rls_region,
    rls_group,
    rls_company,
    rls_businessarea,
    CONCAT(rls_businessarea,'-',rls_company,'-',rls_group) AS rls_key,
    global_id            AS global_id,
    CONCAT(Name, ' ', Surname)    AS full_name,
    UPPER(gender)             AS gender,
	NULL			 	AS age,
	position			AS position,
	employee_status AS employee_status,
    CAST(cost_center_code  AS CHAR)     AS cost_center,
    payroll_company      AS payroll_company,
    actual_working_country AS actual_working_country,
    employee_city        AS actual_working_city,
    'WHITE COLLAR'       AS collar_type,  
    a_level              AS a_level,
    b_level              AS b_level,
    c_level              AS c_level,
    d_level              AS d_level,
    e_level              AS e_level, 
    'dm__hr_kpi_t_dim_employees' source
FROM {{ ref('dm__hr_kpi_t_dim_employees') }}
where employee_status=N'AKTİF'

UNION 

SELECT
	rls_region = 'EUR',
	rls_group = 'BNGROUP_EUR',
	rls_company = 'NS_BLN_EUR',
	rls_businessarea = '_EUR',	
    CONCAT('_EUR','-','NS_BLN_EUR','-','BNGROUP_EUR') AS rls_key,
    Global_Employee_ID   AS global_id,
    CONCAT(Name, ' ', Surname)    AS name,
    CASE 
		WHEN Gender='M' THEN 'ERKEK'
		ELSE 'KADIN'
	END AS gender,
	age,
	NULL				AS position,
	N'AKTİF'      AS employee_status,
    CAST(cost_center  AS CHAR)          AS cost_center,
    Legal_entity         AS payroll_company,
    Actual_Working_Country AS actual_working_country,
    Actual_Working_City  AS actual_working_city,
    UPPER(Collar_Type)    AS collar_type,
    LevelA_Group         AS a_level,
    LevelB_Company_SubGroup AS b_level,
    LevelC_RegionLevel_BU   AS c_level,
    LevelD_Department_Project AS d_level,
    LevelE_Department_Unit   AS e_level,
    'fact_HumanResourcesRonesans' 
FROM [PRDSYNDW-ONDEMAND.SQL.AZURESYNAPSE.NET].[ronesansdwh].[dbo].[fact_HumanResourcesRonesans]

UNION 

SELECT 
    rls_region,
    rls_group,
    rls_company,
    rls_businessarea,
    CONCAT(rls_businessarea,'-',rls_company,'-',rls_group) AS rls_key,
    sap_id,  --global idsi yok
    full_name,
    UPPER(gender) AS gender ,
    age,
	position			AS position,
    N'AKTİF'     AS  employee_status,
	NULL         AS cost_center,
	'No Payroll'         AS payroll_company,
    NULL		AS actual_working_country,
    NULL			AS actual_working_city,
	 CASE 
		WHEN blue_white_collar='Beyaz Yaka' THEN 'WHITE COLLAR'    
		ELSE 'BLUE COLLAR' --106 tane null data var mavi bastım
	END AS collar_type,  
    employee_group       AS a_level,
    employee_group       AS b_level,
    employee_group       AS c_level,
    employee_group       AS d_level,
    employee_group       AS e_level, 
    'dm__hr_kpi_t_dim_subcontractorpersonnel_transformed'
FROM {{ ref("dm__hr_kpi_t_dim_subcontractorpersonnel_transformed") }}
WHERE transaction_date=CAST(GETDATE() AS DATE)
    and employee_group=N'Taşeron'
    and   transaction_distribution in ( N'Organizasyonel Değişiklik',
                    N'Şirkette yeniden işe giriş',
                    N'Naklen Org. Değişikliği',
                    N'İşe alma',
                    N'Şirket değişikliği')