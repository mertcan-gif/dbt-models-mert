{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
with final_cte as (
SELECT
      [rls_region]
      ,[rls_group]
      ,[rls_company]
      ,[rls_businessarea]
      ,[group]
      ,[company]
      ,[process_type]
      ,[process_id]
      ,[sicil_no]
      ,[code]
      ,[process_start]
      ,[process_end]
      ,[process_user]
      ,[process_status]
      ,[sgf_is_reservation_late]
      ,[sgf_reason_reservation_late]
      ,[process_creation_date]
      ,[sgf_start_date_of_trip]
      ,[sgf_end_date_of_trip]
      ,[sgf_number_of_days]
      ,[process_goal]
      ,[sgf_city]                            
      ,[sgf_account_assignment]
      ,NULL AS [sgf_hotel_name]
      ,NULL AS hotel_no
      ,[vehicle_type]
      ,[yms_status]
      ,[yms_process_id]                                      
      ,[yms_description]
      ,[yms_create_date]
      ,[yms_last_action_date]
      ,[yms_process_no]
      ,[yms_total]
      ,[yms_creator]
      ,[yms_created_for_whom]
      ,[yms_sicil_no]
      ,[yms_company]
      ,[yms_company_description]
      ,[yms_business_area]
      ,[yms_department]
      ,[yms_organisation]
      ,[yms_position]
      ,[yms_manager]
      ,[yms_details]
      ,[overtime_shift_end_time]
      ,[overtime_shift_start_time]
      ,[distribution_rate]
      ,[yms_total_cost]
      ,[yms_expense_type]
      ,[trip_type]
  FROM {{ ref('stg__rmh_kpi_t_fact_foodandtravelexpenses') }}
  union all
  SELECT  
       [rls_region]
      ,[rls_group]
      ,[rls_company]
      ,[rls_businessarea]
      ,[group]
      ,[company]
      ,[process_type]
      ,[kts_processid]
      ,[kts_sicilno]
      ,[kts_doc_data_id]
      ,[Oluşturma Tarihi]
      ,[last_action_date]
      ,[Kimin Adına]
      ,[Durum]
      ,CAST(NULL AS varchar(MAX))
      ,CAST(NULL AS varchar(MAX))
      ,[kts_creation_date]
      ,CAST(NULL AS varchar(MAX))
      ,CAST(NULL AS varchar(MAX))
      ,CAST(NULL AS varchar(MAX))
      ,[kts_reason]
      ,null as [sgf_city]                            
      ,null as [sgf_account_assignment]
      ,null as [sgf_hotel_name]
      ,null as hotel_no
      ,null as [vehicle_type]
      ,null as [yms_status]
      ,null as [yms_process_id]                                      
      ,null as [yms_description]
      ,null as [yms_create_date]
      ,null as [yms_last_action_date]
      ,null as [yms_process_no]
      ,null as [yms_total]
      ,null as [yms_creator]
      ,null as [yms_created_for_whom]
      ,null as [yms_sicil_no]
      ,null as [yms_company]
      ,null as [yms_company_description]
      ,null as [yms_business_area]
      ,null as [yms_department]
      ,null as [yms_organisation]
      ,null as [yms_position]
      ,null as [yms_manager]
      ,null as [yms_details]
      ,null as [overtime_shift_end_time]
      ,null as [overtime_shift_start_time]
      ,null as [distribution_rate]
      ,null as [yms_total_cost]
      ,null as [yms_expense_type]
      ,null as [trip_type]
  FROM {{ ref('stg__rmh_kpi_t_fact_ktsprocesses') }}
  union all
  select
       [rls_region]
      ,[rls_group]
      ,[rls_company]
      ,[rls_businessarea]
      ,[group]
      ,[company]
      ,[process_type]
      ,[mtf_processid]
      ,[mtf_sicilno]
      ,[mtf_doc_data_id]
      ,[Oluşturma Tarihi]
      ,[last_action_date]
      ,[Kimin Adına]
      ,[Durum]
      ,CAST(NULL AS varchar(MAX))
      ,CAST(NULL AS varchar(MAX))
      ,[mtf_creation_date]
      ,CAST(NULL AS varchar(MAX))
      ,CAST(NULL AS varchar(MAX))
      ,CAST(NULL AS varchar(MAX))
      ,[mtf_reason]
      ,null as [sgf_city]                            
      ,null as [sgf_account_assignment]
      ,null as [sgf_hotel_name]
      ,null as hotel_no
      ,null as [vehicle_type]
      ,null as [yms_status]
      ,null as [yms_process_id]                                      
      ,null as [yms_description]
      ,null as [yms_create_date]
      ,null as [yms_last_action_date]
      ,null as [yms_process_no]
      ,null as [yms_total]
      ,null as [yms_creator]
      ,null as [yms_created_for_whom]
      ,null as [yms_sicil_no]
      ,null as [yms_company]
      ,null as [yms_company_description]
      ,null as [yms_business_area]
      ,null as [yms_department]
      ,null as [yms_organisation]
      ,null as [yms_position]
      ,null as [yms_manager]
      ,null as [yms_details]
      ,null as [overtime_shift_end_time]
      ,null as [overtime_shift_start_time]
      ,null as [distribution_rate]
      ,null as [yms_total_cost]
      ,null as [yms_expense_type]
      ,null as [trip_type]
    from {{ ref('stg__rmh_kpi_t_fact_mtfprocesses') }}
  union all
SELECT
       [rls_region]
      ,[rls_group]
      ,[rls_company]
      ,[rls_businessarea]
      ,[group]
      ,[company]
      ,[process_type]
      ,[process_id]
      ,[atf_sicil_no]
      ,[atf_doc_data_id]
      ,[create_date]
      ,[finish_date]
      ,[creator_name]
      ,[status]
      ,CAST(NULL AS varchar(MAX))
      ,CAST(NULL AS varchar(MAX))
      ,CAST(NULL AS varchar(MAX))
      ,request_start_date
      ,request_end_date
      ,CAST(NULL AS varchar(MAX))
      ,[atf_reason]
      ,[atf_city]                          
      ,null as [sgf_account_assignment]
      ,null as [sgf_hotel_name]
      ,null as hotel_no
      ,null as [vehicle_type]
      ,null as [yms_status]
      ,null as [yms_process_id]                                      
      ,null as [yms_description]
      ,null as [yms_create_date]
      ,null as [yms_last_action_date]
      ,null as [yms_process_no]
      ,null as [yms_total]
      ,null as [yms_creator]
      ,null as [yms_created_for_whom]
      ,null as [yms_sicil_no]
      ,null as [yms_company]
      ,null as [yms_company_description]
      ,null as [yms_business_area]
      ,null as [yms_department]
      ,null as [yms_organisation]
      ,null as [yms_position]
      ,null as [yms_manager]
      ,null as [yms_details]
      ,null as [overtime_shift_end_time]
      ,null as [overtime_shift_start_time]
      ,null as [distribution_rate]
      ,null as [yms_total_cost]
      ,null as [yms_expense_type]
      ,null as [trip_type]
  FROM {{ ref('stg__rmh_kpi_t_fact_vehicleleadtimes') }}
),
accom as (
SELECT
        SUM(cost) as cost,
        code,
        process_type,
        accom_invoice_date as ex,
        MIN(accom_invoice_date) OVER (PARTITION BY code, process_type) AS accom_invoice_date
FROM {{ ref('stg__rmh_kpi_t_fact_sgfcosts') }}
where 1=1
      and process_type = 'accommodation'
GROUP BY code,
        process_type,
        accom_invoice_date  
),
accom_2 as (
  SELECT
    SUM(cost) as cost,
    MIN(accom_invoice_date) AS accom_invoice_date,
    code,
    process_type 
from accom
where 1=1
GROUP BY code,
        process_type
),
transport_main as (
SELECT
        SUM(cost) as cost,
        code,
        process_type,
        transport_invoice_date as ex,
        MIN(transport_invoice_date) OVER (PARTITION BY code, process_type) AS transport_invoice_date,
        SUM(coalesce(penalty_cost,0)) as penalty_cost,
        penalty_cost as ex_penalty_cost
FROM {{ ref('stg__rmh_kpi_t_fact_sgfcosts') }}
where 1=1
      and process_type = 'transportation'
GROUP BY code,
        process_type,
        transport_invoice_date,penalty_cost
),
transport as (
SELECT
        SUM(cost) as cost,
        code,
        process_type,
        MIN(transport_invoice_date)  AS transport_invoice_date,
        SUM(coalesce(penalty_cost,0)) as penalty_cost
FROM transport_main
where 1=1
      and process_type = 'transportation'
GROUP BY code,
        process_type
),
transfers as (
SELECT
        SUM(cost) as cost,
        code,
        process_type
FROM {{ ref('stg__rmh_kpi_t_fact_sgfcosts') }}
where 1=1
      and process_type = 'transfer'
GROUP BY code,
        process_type
),
final_cte_2 as (
SELECT
       [rls_region]
      ,[rls_group]
      ,[rls_company]
      ,CONCAT([rls_businessarea],'_TUR') AS rls_businessarea
      ,[group]
      ,[company]
      ,[rls_businessarea] as business_area
      ,case
          when   overtime_shift_start_time IS NOT NULL AND overtime_shift_end_time IS NOT NULL THEN 'Mesai'
          when   process_id IS NULL and overtime_shift_end_time IS null THEN 'YMS'
        ELSE g.process_type end as process_type
      ,[process_id]
      ,[sicil_no]
      ,coalesce(g.[code],[yms_process_no]) as code
      ,[process_start]
      ,[process_end]
      ,[process_user]
      ,[process_status]
      ,[sgf_is_reservation_late]
      ,null as [sgf_reason_reservation_late]
      ,[process_creation_date]
      ,[sgf_start_date_of_trip]
      ,[sgf_end_date_of_trip]
      ,[sgf_number_of_days]
      ,NULL AS [process_goal]
      ,[sgf_city]                            
      ,[sgf_account_assignment]
      ,NULL AS sgf_hotel_name  --,STRING_AGG([sgf_hotel_name], ', ') as sgf_hotel_name
      ,NULL AS hotel_no  --STRING_AGG([hotel_no], ', ') as hotel_no
      ,[vehicle_type]
      ,[yms_status]
      ,[yms_process_id]                                      
      ,NULL AS [yms_description]
      ,[yms_create_date]
      ,[yms_last_action_date]
      ,[yms_process_no]
      ,[yms_total]
      ,[yms_creator]
      ,[yms_created_for_whom]
      ,[yms_sicil_no]
      ,[yms_company]
      ,NULL AS [yms_company_description]
      ,[yms_business_area]
     ,NULL AS [yms_department]
     ,NULL AS [yms_organisation]
     ,NULL AS [yms_position]
      ,[yms_manager]
      ,[yms_details]
      ,[overtime_shift_end_time]
      ,[overtime_shift_start_time]
      ,[distribution_rate]
      ,ss.cost as accommodation_total_cost_tl
      ,ss.cost*g.distribution_rate/100 as accommodation_distributed_cost_tl
      ,st.cost as transportation_total_cost_tl
      ,st.cost*g.distribution_rate/100 as transportation_distributed_cost_tl
      ,sa.cost as transfer_total_cost_tl
      ,sa.cost*g.distribution_rate/100 as transfer_total_cost_distributed_tl
      ,4 as standard_lead_time
      ,trip_type as sgf_trip_type
      ,[yms_total_cost] as yms_total_cost_tl
      ,[yms_total_cost] as yms_total_cost_distributed_tl
      ,[yms_expense_type]
     -- ,average_original_amount = st.cost - st.bonus
     -- ,average_original_amount_distributed = (st.cost - st.bonus)*g.distribution_rate/100
    --  ,at.[average_rmh_salary]
    --  ,at.[average_rmh_salary]*g.distribution_rate/100 as average_rmh_salary_distributed
    --  ,st.bonus
    --  ,st.bonus*g.distribution_rate/100 as thirdparty_bonus_distributed
      ,cast(vld.txtApproximateCost as float) as vehicle_cost_tl
      ,cast(vld.txtApproximateCost as float)*g.distribution_rate/100 as vehicle_cost_distributed_tl
      ,cur.usd_value
      ,cur.eur_value
      ,st.penalty_cost as transportation_penalty_cost_tl
      ,st.penalty_cost*g.distribution_rate/100 AS transportation_distributed_penalty_cost_tl
      ,st.transport_invoice_date as invoice_date_transport
      ,ss.accom_invoice_date as invoice_date_accom
FROM final_cte g
  left join  accom_2 ss  on rtrim(ltrim(ss.code)) collate database_default = g.code collate database_default and ss.process_type = 'accommodation'
  left join  transport st  on rtrim(ltrim(st.code)) collate database_default = g.code collate database_default and st.process_type = 'transportation'
  left join  transfers sa  on rtrim(ltrim(sa.code)) collate database_default = g.code collate database_default and sa.process_type = 'transfer'
--  LEFT JOIN  {{ ref('stg__rmh_kpi_t_fact_averagetransportationcosts') }} at on CONCAT(YEAR(at.[Date]),MONTH(at.[Date])) = CONCAT(YEAR(g.[sgf_start_date_of_trip]),MONTH(g.[sgf_start_date_of_trip])) AND g.process_type = 'SGF'
  LEFT JOIN  eba.eba.[dbo].[E_Z206VehicleRequest_Form] vld on vld.cmbSgfForm = g.process_id and g.process_type = 'SGF'
  AND vld.cmbSgfForm_TEXT IS NOT NULL
  AND vld.cmbSgfForm_TEXT != ''
  AND vld.docDataId is not null
  AND vld.docDataId != ''
  AND vld.txtApproximateCost is not null
  LEFT join {{ ref('dm__dimensions_t_dim_dailys4currencies') }} cur on CAST(cur.[date_value] as date) = CAST(g.[sgf_start_date_of_trip] as DATE) and cur.currency= 'TRY'

 group by
       [rls_region]
      ,[rls_group]
      ,[rls_company]
      ,[rls_businessarea]
      ,[group]
      ,[company]
      ,g.[process_type]
      ,[process_id]
      ,[sicil_no]
      ,g.[code]
      ,[process_start]
      ,[process_end]
      ,[process_user]
      ,[process_status]
      ,[sgf_is_reservation_late]
  --    , NULL AS [sgf_reason_reservation_late]
      ,[process_creation_date]
      ,[sgf_start_date_of_trip]
      ,[sgf_end_date_of_trip]
      ,[sgf_number_of_days]
  --    ,NULL AS [process_goal]
      ,[sgf_city]                            
      ,[sgf_account_assignment]
      ,[vehicle_type]
      ,[yms_status]
      ,[yms_process_id]                                      
      --,NULL AS [yms_description]
      ,[yms_create_date]
      ,[yms_last_action_date]
      ,[yms_process_no]
      ,[yms_total]
      ,[yms_creator]
      ,[yms_created_for_whom]
      ,[yms_sicil_no]
      ,[yms_company]
 --     ,NULL AS [yms_company_description]
      ,[yms_business_area]
  --   ,NULL AS [yms_department]
  --   ,NULL AS [yms_organisation]
  --   ,NULL AS [yms_position]
      ,[yms_manager]
      ,[yms_details]
      ,[overtime_shift_end_time]
      ,[overtime_shift_start_time]
      ,[distribution_rate]
      ,ss.cost
      ,st.cost
      ,sa.cost
    --  ,st.bonus
      ,trip_type
      ,yms_total_cost
      ,yms_expense_type
   --   ,at.[average_rmh_salary]
      ,vld.txtApproximateCost
      ,cur.usd_value
      ,cur.eur_value
      ,st.transport_invoice_date
      ,ss.accom_invoice_date
      ,st.penalty_cost
)
SELECT
*
FROM final_cte_2
where 1=1
    AND process_type != 'SGF'
UNION ALL
SELECT
*
FROM final_cte_2
WHERE 1=1
    AND process_type = 'SGF'
    --and (invoice_date_transport is not null or invoice_date_accom is not null )
    --AND (accommodation_distributed_cost_tl is not null or transportation_distributed_cost_tl is not null or transfer_total_cost_distributed_tl is not null)
