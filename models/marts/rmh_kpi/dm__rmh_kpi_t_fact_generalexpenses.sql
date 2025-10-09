{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}


select 
         [rls_region]
        ,[rls_group]
        ,[rls_company]
        ,[rls_businessarea]
        ,[group]
        ,[company]
        ,[business_area]
        ,[process_type]
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
        ,[hotel_no]
        ,[vehicle_type]
        ,[yms_status]
        ,[yms_description]
        ,[yms_create_date]
        ,[yms_last_action_date]
        ,[yms_process_no]
        ,[yms_company_description]
        ,[standard_lead_time]
        ,[sgf_trip_type]
        ,[invoice_date_accom]
        ,[invoice_date_transport]
        ,[accommodation_distributed_cost_tl]
        ,[transportation_distributed_cost_tl]
        ,[transfer_total_cost_distributed_tl]
        ,[yms_total_cost_distributed_tl]
        ,[vehicle_cost_distributed_tl]
        ,[transportation_distributed_penalty_cost_tl]
        ,[yms_details]
from {{ ref('stg__rmh_kpi_t_fact_generalexpenses') }}
where 1=1
	and company is not null
  and code not in (
           'KTS-2019-78'
          ,'KTS-2019-10'
          ,'KTS-2019-16'
          ,'KTS-2019-24'
          ,'KTS-2019-118'
          ,'KTS-2019-185'
          ,'SGF-2023-8571')