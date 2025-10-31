-- tests/assert_no_invalid_date_logic.sql
-- This test will fail if any employee's end date is before their start date.
select
    user_id,
    start_date,
    end_date
from {{ ref('dim_employees') }}
where end_date < start_date


-- tests/assert_employee_is_not_own_manager.sql
-- This test will fail if an employee's user_id is the same as their manager's user_id.
select
    user_id,
    manager_user_id
from {{ ref('dim_employees') }}
where user_id = manager_user_id


-- tests/assert_no_placeholder_birth_dates.sql
-- This test will fail if any employee has a birth date of '1900-01-01', which is a common placeholder.
select
    user_id,
    date_of_birth
from {{ ref('dim_employees') }}
where date_of_birth = '1900-01-01'
