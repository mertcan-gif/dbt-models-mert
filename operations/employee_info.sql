WITH CurrentEmployees AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY start_date DESC, seq_number DESC) as rn
    FROM
        aws_stage.sf_odata.raw__hr_kpi_t_sf_newsf_employees
),
EducationCheck AS (
    SELECT DISTINCT user_id
    FROM aws_stage.sf_odata.raw__hr_kpi_t_sf_newsf_employee_education
),
LanguageCheck AS (
    SELECT DISTINCT user_id
    FROM aws_stage.sf_odata.raw__hr_kpi_t_sf_newsf_languages
),
ExperienceCheck AS (
    SELECT user_id FROM aws_stage.sf_odata.raw__hr_kpi_t_sf_newsf_insideworkexperience
    UNION
    SELECT user_id FROM aws_stage.sf_odata.raw__hr_kpi_t_sf_newsf_outsideworkexperience
)
SELECT
    emp.email_address AS employee,
    hrbp.email_address AS direct_hrbp,
    hrbp_lead.email_address AS hrbp_lead,
    CASE
        WHEN edu.user_id IS NOT NULL THEN 'OK'
        ELSE 'Yok'
    END AS [Education Verisi],
    CASE
        WHEN lang.user_id IS NOT NULL THEN 'OK'
        ELSE 'Yok'
    END AS [Dil Verisi],
    CASE
        WHEN exp.user_id IS NOT NULL THEN 'OK'
        ELSE 'Yok'
    END AS [Experience Verisi]
FROM
    CurrentEmployees AS emp
LEFT JOIN
    CurrentEmployees AS hrbp
    ON emp.hr_responsible_sf_id = hrbp.user_id AND hrbp.rn = 1
LEFT JOIN
    CurrentEmployees AS hrbp_lead
    ON hrbp.manager_user_id = hrbp_lead.user_id AND hrbp_lead.rn = 1
LEFT JOIN
    EducationCheck AS edu
    ON emp.user_id = edu.user_id
LEFT JOIN
    LanguageCheck AS lang
    ON emp.user_id = lang.user_id
LEFT JOIN
    ExperienceCheck AS exp
    ON emp.user_id = exp.user_id
WHERE
    emp.rn = 1
    AND emp.email_address IS NOT NULL
    AND hrbp.email_address IS NOT NULL;     