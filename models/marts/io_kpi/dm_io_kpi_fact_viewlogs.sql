{% set emp_cols = adapter.get_columns_in_relation(ref('dm__hr_kpi_t_dim_employees')) %}
{% set exclude_cols = ['rls_region','rls_group','rls_company','rls_businessarea'] %}

{% set selected_cols = [] %}
{% for col in emp_cols %}
    {% if col.name not in exclude_cols %}
        {% do selected_cols.append("emp." ~ col.name) %}
    {% endif %}
{% endfor %}

{{ config(
    materialized = 'table',
    tags = ['metadata_kpi','rmore']
) }}

SELECT
    logs.rls_region,
    logs.rls_group,
    logs.rls_company,
    logs.rls_businessarea,
    logs.id,
    logs.creation_time,
    creation_date=cast(logs.creation_time as date),
    logs.user_id AS log_user_id,
    logs.workspace_id,
    logs.report_id,
    logs.report_name,
    logs.report_type,
    logs.consumption_method,
    logs.transaction_amount,
    {{ selected_cols | join(', ') }}
FROM {{ ref('dm__metadata_kpi_t_fact_viewreportlogs') }} logs
LEFT JOIN {{ ref('dm__hr_kpi_t_dim_employees') }} emp
    ON logs.user_id = emp.email_address
