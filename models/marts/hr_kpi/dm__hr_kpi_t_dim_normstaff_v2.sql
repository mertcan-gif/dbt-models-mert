{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}

-- 2025-02-17 Adem Numan Kaya: RLS_BusinessArea gerçeğe göre oluşturulmadı. Eğer tabloya RLS Business Area isteniyorsa düzenlenmesi gerekli.   

WITH matrix_relationship_dimension AS (

	SELECT 
		*
		,ROW_NUMBER() OVER(PARTITION BY position_code ORDER BY created_date DESC,last_modified_date DESC, matrix_relationship_type DESC) RN
	FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_position_matrix_relationship') }}
)

,norm_staff_cte AS (

SELECT emp.[code]
      ,[position_name]
      ,[job_function]
      ,[start_date]  = CAST([start_date] AS DATE)
      ,[end_date]  = CAST([end_date] AS DATE) -- ED HR ile karşılaştırmak için eklendi
      ,[planned_start_date] =   CAST([planned_start_date] AS DATE)
      ,[planned_end_date] =  CAST([planned_end_date] AS DATE)
      ,[position_planned_date] =  CAST([position_planned_date] AS DATE)
      ,[planned_recruitment_date_budget] =  CAST([planned_recruitment_date_budget] AS DATE)
      ,[planned_recruitment_date] =  CAST([planned_recruitment_date] AS DATE)
      ,[planned_end_date_budget] =  CAST([planned_end_date_budget] AS DATE)
      ,[user_id]
      ,case when [global_id] = 'None' then '' else [global_id] end as [global_id]
      ,case when [sap_id] = 'None' then '' else [sap_id] end as [sap_id]
      ,case when [incumbent_name] = 'null null' then '' else [incumbent_name] end as [incumbent_name]
      ,[employee_type]
      ,is_vacant -- ED HR ile karşılaştırmak için eklendi
      ,level_a_description = [level_a].name_tr
      ,level_b_description = [level_b].name_tr
      ,level_c_description = [level_c].name_tr
      ,level_d_description = [level_d].name_tr
      ,level_e_description = [level_e].name_tr
      ,case when [parent_department] = 'None' then '' else [parent_department] end as [parent_department]
      ,[company]
      ,[company_code]
	  ,employee_sub_area
      ,[physical_location]
      ,[cost_center]
      ,[cost_center_code]
      ,[is_gyg_position]
      ,[work_place_type]
      ,[parent_position_name]
      ,case when [parent_position_incumbent_name] = 'null null' then '' else [parent_position_incumbent_name] end as [parent_position_incumbent_name]
      ,case when [parent_position_user_id] = 'None' then '' else [parent_position_user_id]  end as [parent_position_user_id] 
      ,[matrix_relationship_type] = CASE 
										WHEN emp.[matrix_relationship_type] = 'None' THEN '' 
										WHEN (emp.[matrix_relationship_type] = N'Fonksiyonel Yönetici' OR emp.[matrix_relationship_type] = N'custom manager')
											AND (mrd.[matrix_relationship_type] = N'Fonksiyonel Yönetici' OR mrd.[matrix_relationship_type] = N'custom manager') THEN ''
										WHEN (emp.[matrix_relationship_type] = N'Fonksiyonel Yönetici' OR emp.[matrix_relationship_type] = N'custom manager')
											AND (mrd.[matrix_relationship_type] <> N'Fonksiyonel Yönetici' AND mrd.[matrix_relationship_type] <> N'custom manager') THEN mrd.[matrix_relationship_type]
										ELSE emp.[matrix_relationship_type]
									END
	   ,[related_position_code] = CASE 
										WHEN emp.[matrix_relationship_type] = 'None' THEN '' 
										WHEN (emp.[matrix_relationship_type] = N'Fonksiyonel Yönetici' OR emp.[matrix_relationship_type] = N'custom manager')
											AND (mrd.[matrix_relationship_type] = N'Fonksiyonel Yönetici' OR mrd.[matrix_relationship_type] = N'custom manager') THEN ''
										WHEN (emp.[matrix_relationship_type] = N'Fonksiyonel Yönetici' OR emp.[matrix_relationship_type] = N'custom manager')
											AND (mrd.[matrix_relationship_type] <> N'Fonksiyonel Yönetici' AND mrd.[matrix_relationship_type] <> N'custom manager') THEN mrd.[related_position_code]
										ELSE emp.[related_position_code]
								   END
	  ,emp.last_modified_date
      --,emp.ronesans_rank -- Rank bilgisi su an icin gonderilmemektedir. 
      ,is_latest = CASE 
                    WHEN CAST(END_DATE AS DATE) = '9999-12-31' THEN 'Latest'
                    ELSE 'Previous' 
                  END

FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_positions_historia') }} emp 
		LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_a') }} level_a ON level_a.code = emp.a_level
		LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_b') }} level_b ON level_b.code = emp.b_level
		LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_c') }} level_c ON level_c.code = emp.c_level
		LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_d') }} level_d ON level_d.code = emp.d_level
		LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_e') }} level_e ON level_e.code = emp.e_level
		LEFT JOIN matrix_relationship_dimension mrd ON emp.code = mrd.position_code AND mrd.RN = 1
where level_a.name_tr <> N'MÜHENDİSLİK HİZMETLERİ'
)

/* Create a date series of 24 months fixed from 2025-01 to 2026-12 */
, date_series AS (
    SELECT
        DATEADD(MONTH, n.n, DATEFROMPARTS(2025, 1, 1)) AS month_date
    FROM (
        SELECT TOP 24
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM sys.objects a
        CROSS JOIN sys.objects b
    ) n
)

/* Base query with RLS info */
, base_query AS (
    SELECT 
        rls_region = CASE 
                        WHEN (SELECT TOP 1 custom_region FROM "aws_stage"."hr_kpi"."raw__hr_kpi_t_dim_group" grp WHERE grp.[group] = level_a_description COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR'
                        ELSE 'RUS' 
                    END
        ,rls_group = CONCAT(
                            (SELECT TOP 1 [group_rls] FROM "aws_stage"."hr_kpi"."raw__hr_kpi_t_dim_group" grp WHERE grp.[group] = level_a_description COLLATE DATABASE_DEFAULT) ,'_',
                            CASE 
                                WHEN (SELECT TOP 1 custom_region FROM "aws_stage"."hr_kpi"."raw__hr_kpi_t_dim_group" grp WHERE grp.[group] = level_a_description COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR'
                                ELSE 'RUS' 
                            END
                            )
        ,rls_company =  CONCAT(
                            UPPER(level_b_description),
                            '_',
                            CASE 
                                WHEN (SELECT TOP 1 custom_region FROM "aws_stage"."hr_kpi"."raw__hr_kpi_t_dim_group" grp WHERE grp.[group] = level_a_description COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR'
                                ELSE 'RUS' 
                            END
                            )
        ,rls_businessarea = CONCAT(
                            '_',
                            CASE 
                                WHEN (SELECT TOP 1 custom_region FROM "aws_stage"."hr_kpi"."raw__hr_kpi_t_dim_group" grp WHERE grp.[group] = level_a_description COLLATE DATABASE_DEFAULT) = 'TR' THEN 'TUR'
                                ELSE 'RUS' 
                            END
                            )
        ,ns.*
    FROM norm_staff_cte ns
    WHERE 1=1
    {# and employee_type not in (N'Hayalet Kullanıcı', N'Stajyer-TOBB', 'Stajyer-Üniversite-Teknik', N'Stajyer-Üniversite-Diğer') #}
    /*
    2025-03-07 ANK: SF'ten alınan Norm Kadro Raporu ile uyuşması filtre kaldırılmıştır. 

    NOT(is_vacant=0 and incumbent_name='') --Parmanent Staff sorgusu ile aynı olması için oradaki filtreler buraya da eklendi. 
    */
)

/* Final result with unpivoted date structure */
SELECT 
    bq.code,
    bq.related_position_code,
    date = FORMAT(ds.month_date, 'yyyy-MM'),
    is_norm = CASE 
                WHEN ds.month_date >= bq.planned_start_date 
                AND ds.month_date <= bq.planned_end_date
                AND ds.month_date <= bq.end_date
                AND ds.month_date >= bq.start_date
                THEN 1
                ELSE 0
             END,
    bq.rls_region,
    bq.rls_group,
    bq.rls_company,
    bq.rls_businessarea,
    bq.position_name,
    bq.job_function,
    bq.start_date,
    bq.end_date,
    bq.planned_start_date,
    bq.planned_end_date,
    bq.user_id,
    bq.global_id,
    bq.sap_id,
    bq.incumbent_name,
    bq.employee_type,
    bq.is_vacant,
    bq.level_a_description,
    bq.level_b_description,
    bq.level_c_description,
    bq.level_d_description,
    bq.level_e_description,
    bq.parent_department,
    bq.company,
    bq.company_code,
    bq.employee_sub_area,
    bq.physical_location,
    bq.cost_center,
    bq.cost_center_code,
    bq.is_gyg_position,
    bq.work_place_type,
    bq.parent_position_name,
    bq.parent_position_incumbent_name,
    bq.parent_position_user_id,
    bq.matrix_relationship_type,
    bq.last_modified_date,
    bq.is_latest
FROM 
    base_query bq
CROSS JOIN 
    date_series ds