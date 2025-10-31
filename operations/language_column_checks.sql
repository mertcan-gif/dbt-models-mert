-- =================================================================================
-- Missing values & Language Consistency for columns with locale and english languages.
-- =================================================================================


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_job_application_education_newsf_rmore
-- =================================================================================

-- Missing Value Checks
SELECT 'country_tr' AS column_name, SUM(CASE WHEN country_tr IS NULL OR country_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_application_education_newsf_rmore;
SELECT 'country_en' AS column_name, SUM(CASE WHEN country_en IS NULL OR country_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_application_education_newsf_rmore;
SELECT 'graduation_tr' AS column_name, SUM(CASE WHEN graduation_tr IS NULL OR graduation_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_application_education_newsf_rmore;
SELECT 'graduation_en' AS column_name, SUM(CASE WHEN graduation_en IS NULL OR graduation_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_application_education_newsf_rmore;
SELECT 'name_of_school_tr' AS column_name, SUM(CASE WHEN name_of_school_tr IS NULL OR name_of_school_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_application_education_newsf_rmore;
SELECT 'name_of_school_en' AS column_name, SUM(CASE WHEN name_of_school_en IS NULL OR name_of_school_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_application_education_newsf_rmore;

-- Language Consistency Checks
SELECT 'country' AS base_name, COUNT(DISTINCT country_tr) AS tr_distinct_count, COUNT(DISTINCT country_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_job_application_education_newsf_rmore;
SELECT 'graduation' AS base_name, COUNT(DISTINCT graduation_tr) AS tr_distinct_count, COUNT(DISTINCT graduation_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_job_application_education_newsf_rmore;
SELECT 'name_of_school' AS base_name, COUNT(DISTINCT name_of_school_tr) AS tr_distinct_count, COUNT(DISTINCT name_of_school_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_job_application_education_newsf_rmore;


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_job_application_languages_newsf_rmore
-- =================================================================================

-- Missing Value Checks
SELECT 'language_tr' AS column_name, SUM(CASE WHEN language_tr IS NULL OR language_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_application_languages_newsf_rmore;
SELECT 'language_en' AS column_name, SUM(CASE WHEN language_en IS NULL OR language_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_application_languages_newsf_rmore;
SELECT 'reading_tr' AS column_name, SUM(CASE WHEN reading_tr IS NULL OR reading_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_application_languages_newsf_rmore;
SELECT 'reading_en' AS column_name, SUM(CASE WHEN reading_en IS NULL OR reading_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_application_languages_newsf_rmore;
SELECT 'writing_tr' AS column_name, SUM(CASE WHEN writing_tr IS NULL OR writing_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_application_languages_newsf_rmore;
SELECT 'writing_en' AS column_name, SUM(CASE WHEN writing_en IS NULL OR writing_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_application_languages_newsf_rmore;

-- Language Consistency Checks
SELECT 'language' AS base_name, COUNT(DISTINCT language_tr) AS tr_distinct_count, COUNT(DISTINCT language_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_job_application_languages_newsf_rmore;
SELECT 'reading' AS base_name, COUNT(DISTINCT reading_tr) AS tr_distinct_count, COUNT(DISTINCT reading_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_job_application_languages_newsf_rmore;
SELECT 'writing' AS base_name, COUNT(DISTINCT writing_tr) AS tr_distinct_count, COUNT(DISTINCT writing_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_job_application_languages_newsf_rmore;


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_job_applications_newsf_rmore
-- =================================================================================

-- Missing Value Checks
SELECT 'gorusen_kisi_ik_tr' AS column_name, SUM(CASE WHEN gorusen_kisi_ik_tr IS NULL OR gorusen_kisi_ik_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'gorusen_kisi_ik_en' AS column_name, SUM(CASE WHEN gorusen_kisi_ik_en IS NULL OR gorusen_kisi_ik_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'gorusen_kisi_ik_ygf_iki_tr' AS column_name, SUM(CASE WHEN gorusen_kisi_ik_ygf_iki_tr IS NULL OR gorusen_kisi_ik_ygf_iki_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'gorusen_kisi_ik_ygf_iki_en' AS column_name, SUM(CASE WHEN gorusen_kisi_ik_ygf_iki_en IS NULL OR gorusen_kisi_ik_ygf_iki_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'gorusen_kisi_ik_ygf_tr' AS column_name, SUM(CASE WHEN gorusen_kisi_ik_ygf_tr IS NULL OR gorusen_kisi_ik_ygf_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'gorusen_kisi_ik_ygf_en' AS column_name, SUM(CASE WHEN gorusen_kisi_ik_ygf_en IS NULL OR gorusen_kisi_ik_ygf_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'gorusen_kisi_ik_ygf_uc_tr' AS column_name, SUM(CASE WHEN gorusen_kisi_ik_ygf_uc_tr IS NULL OR gorusen_kisi_ik_ygf_uc_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'gorusen_kisi_ik_ygf_uc_en' AS column_name, SUM(CASE WHEN gorusen_kisi_ik_ygf_uc_en IS NULL OR gorusen_kisi_ik_ygf_uc_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'oneren_yetkili_tr' AS column_name, SUM(CASE WHEN oneren_yetkili_tr IS NULL OR oneren_yetkili_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'oneren_yetkili_en' AS column_name, SUM(CASE WHEN oneren_yetkili_en IS NULL OR oneren_yetkili_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'source_tr' AS column_name, SUM(CASE WHEN source_tr IS NULL OR source_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'source_en' AS column_name, SUM(CASE WHEN source_en IS NULL OR source_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;

-- Language Consistency Checks
SELECT 'gorusen_kisi_ik' AS base_name, COUNT(DISTINCT gorusen_kisi_ik_tr) AS tr_distinct_count, COUNT(DISTINCT gorusen_kisi_ik_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'gorusen_kisi_ik_ygf_iki' AS base_name, COUNT(DISTINCT gorusen_kisi_ik_ygf_iki_tr) AS tr_distinct_count, COUNT(DISTINCT gorusen_kisi_ik_ygf_iki_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'gorusen_kisi_ik_ygf' AS base_name, COUNT(DISTINCT gorusen_kisi_ik_ygf_tr) AS tr_distinct_count, COUNT(DISTINCT gorusen_kisi_ik_ygf_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'gorusen_kisi_ik_ygf_uc' AS base_name, COUNT(DISTINCT gorusen_kisi_ik_ygf_uc_tr) AS tr_distinct_count, COUNT(DISTINCT gorusen_kisi_ik_ygf_uc_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'oneren_yetkili' AS base_name, COUNT(DISTINCT oneren_yetkili_tr) AS tr_distinct_count, COUNT(DISTINCT oneren_yetkili_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;
SELECT 'source' AS base_name, COUNT(DISTINCT source_tr) AS tr_distinct_count, COUNT(DISTINCT source_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore;


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_newsf_employee_assign_items
-- =================================================================================

-- Missing Value Checks
SELECT 'assign_item_tr' AS column_name, SUM(CASE WHEN assign_item_tr IS NULL OR assign_item_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employee_assign_items;
SELECT 'assign_item_en' AS column_name, SUM(CASE WHEN assign_item_en IS NULL OR assign_item_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employee_assign_items;

-- Language Consistency Checks
SELECT 'assign_item' AS base_name, COUNT(DISTINCT assign_item_tr) AS tr_distinct_count, COUNT(DISTINCT assign_item_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employee_assign_items;


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_newsf_employee_education
-- =================================================================================

-- Missing Value Checks
SELECT 'field_of_study_tr' AS column_name, SUM(CASE WHEN field_of_study_tr IS NULL OR field_of_study_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employee_education;
SELECT 'field_of_study_en' AS column_name, SUM(CASE WHEN field_of_study_en IS NULL OR field_of_study_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employee_education;
SELECT 'graduation_tr' AS column_name, SUM(CASE WHEN graduation_tr IS NULL OR graduation_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employee_education;
SELECT 'graduation_en' AS column_name, SUM(CASE WHEN graduation_en IS NULL OR graduation_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employee_education;
SELECT 'name_of_school_tr' AS column_name, SUM(CASE WHEN name_of_school_tr IS NULL OR name_of_school_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employee_education;
SELECT 'name_of_school_en' AS column_name, SUM(CASE WHEN name_of_school_en IS NULL OR name_of_school_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employee_education;

-- Language Consistency Checks
SELECT 'field_of_study' AS base_name, COUNT(DISTINCT field_of_study_tr) AS tr_distinct_count, COUNT(DISTINCT field_of_study_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employee_education;
SELECT 'graduation' AS base_name, COUNT(DISTINCT graduation_tr) AS tr_distinct_count, COUNT(DISTINCT graduation_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employee_education;
SELECT 'name_of_school' AS base_name, COUNT(DISTINCT name_of_school_tr) AS tr_distinct_count, COUNT(DISTINCT name_of_school_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employee_education;


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_newsf_employees
-- =================================================================================

-- Missing Value Checks
SELECT 'employee_city_tr' AS column_name, SUM(CASE WHEN employee_city_tr IS NULL OR employee_city_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'employee_city_en' AS column_name, SUM(CASE WHEN employee_city_en IS NULL OR employee_city_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'employee_status_tr' AS column_name, SUM(CASE WHEN employee_status_tr IS NULL OR employee_status_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'employee_status_en' AS column_name, SUM(CASE WHEN employee_status_en IS NULL OR employee_status_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'employee_type_name_tr' AS column_name, SUM(CASE WHEN employee_type_name_tr IS NULL OR employee_type_name_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'employee_type_name_en' AS column_name, SUM(CASE WHEN employee_type_name_en IS NULL OR employee_type_name_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'real_termination_reason_tr' AS column_name, SUM(CASE WHEN real_termination_reason_tr IS NULL OR real_termination_reason_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'real_termination_reason_en' AS column_name, SUM(CASE WHEN real_termination_reason_en IS NULL OR real_termination_reason_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'report_organization_tr' AS column_name, SUM(CASE WHEN report_organization_tr IS NULL OR report_organization_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'report_organization_en' AS column_name, SUM(CASE WHEN report_organization_en IS NULL OR report_organization_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'ronesans_rank_personal_tr' AS column_name, SUM(CASE WHEN ronesans_rank_personal_tr IS NULL OR ronesans_rank_personal_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'ronesans_rank_personal_en' AS column_name, SUM(CASE WHEN ronesans_rank_personal_en IS NULL OR ronesans_rank_personal_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'ronesans_rank_tr' AS column_name, SUM(CASE WHEN ronesans_rank_tr IS NULL OR ronesans_rank_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'ronesans_rank_en' AS column_name, SUM(CASE WHEN ronesans_rank_en IS NULL OR ronesans_rank_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'workplace_tr' AS column_name, SUM(CASE WHEN workplace_tr IS NULL OR workplace_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'workplace_en' AS column_name, SUM(CASE WHEN workplace_en IS NULL OR workplace_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;

-- Language Consistency Checks
SELECT 'employee_city' AS base_name, COUNT(DISTINCT employee_city_tr) AS tr_distinct_count, COUNT(DISTINCT employee_city_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'employee_status' AS base_name, COUNT(DISTINCT employee_status_tr) AS tr_distinct_count, COUNT(DISTINCT employee_status_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'employee_type_name' AS base_name, COUNT(DISTINCT employee_type_name_tr) AS tr_distinct_count, COUNT(DISTINCT employee_type_name_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'real_termination_reason' AS base_name, COUNT(DISTINCT real_termination_reason_tr) AS tr_distinct_count, COUNT(DISTINCT real_termination_reason_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'report_organization' AS base_name, COUNT(DISTINCT report_organization_tr) AS tr_distinct_count, COUNT(DISTINCT report_organization_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'ronesans_rank_personal' AS base_name, COUNT(DISTINCT ronesans_rank_personal_tr) AS tr_distinct_count, COUNT(DISTINCT ronesans_rank_personal_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'ronesans_rank' AS base_name, COUNT(DISTINCT ronesans_rank_tr) AS tr_distinct_count, COUNT(DISTINCT ronesans_rank_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;
SELECT 'workplace' AS base_name, COUNT(DISTINCT workplace_tr) AS tr_distinct_count, COUNT(DISTINCT workplace_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees;


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_newsf_employees_historia
-- =================================================================================

-- Missing Value Checks
SELECT 'employee_status_tr' AS column_name, SUM(CASE WHEN employee_status_tr IS NULL OR employee_status_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'employee_status_en' AS column_name, SUM(CASE WHEN employee_status_en IS NULL OR employee_status_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'employee_type_name_tr' AS column_name, SUM(CASE WHEN employee_type_name_tr IS NULL OR employee_type_name_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'employee_type_name_en' AS column_name, SUM(CASE WHEN employee_type_name_en IS NULL OR employee_type_name_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'position_city_tr' AS column_name, SUM(CASE WHEN position_city_tr IS NULL OR position_city_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'position_city_en' AS column_name, SUM(CASE WHEN position_city_en IS NULL OR position_city_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'real_termination_reason_tr' AS column_name, SUM(CASE WHEN real_termination_reason_tr IS NULL OR real_termination_reason_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'real_termination_reason_en' AS column_name, SUM(CASE WHEN real_termination_reason_en IS NULL OR real_termination_reason_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'report_organization_tr' AS column_name, SUM(CASE WHEN report_organization_tr IS NULL OR report_organization_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'report_organization_en' AS column_name, SUM(CASE WHEN report_organization_en IS NULL OR report_organization_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'ronesans_rank_personal_tr' AS column_name, SUM(CASE WHEN ronesans_rank_personal_tr IS NULL OR ronesans_rank_personal_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'ronesans_rank_personal_en' AS column_name, SUM(CASE WHEN ronesans_rank_personal_en IS NULL OR ronesans_rank_personal_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'ronesans_rank_tr' AS column_name, SUM(CASE WHEN ronesans_rank_tr IS NULL OR ronesans_rank_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'ronesans_rank_en' AS column_name, SUM(CASE WHEN ronesans_rank_en IS NULL OR ronesans_rank_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'work_area_tr' AS column_name, SUM(CASE WHEN work_area_tr IS NULL OR work_area_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'work_area_en' AS column_name, SUM(CASE WHEN work_area_en IS NULL OR work_area_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'workplace_tr' AS column_name, SUM(CASE WHEN workplace_tr IS NULL OR workplace_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'workplace_en' AS column_name, SUM(CASE WHEN workplace_en IS NULL OR workplace_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;

-- Language Consistency Checks
SELECT 'employee_status' AS base_name, COUNT(DISTINCT employee_status_tr) AS tr_distinct_count, COUNT(DISTINCT employee_status_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'employee_type_name' AS base_name, COUNT(DISTINCT employee_type_name_tr) AS tr_distinct_count, COUNT(DISTINCT employee_type_name_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'position_city' AS base_name, COUNT(DISTINCT position_city_tr) AS tr_distinct_count, COUNT(DISTINCT position_city_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'real_termination_reason' AS base_name, COUNT(DISTINCT real_termination_reason_tr) AS tr_distinct_count, COUNT(DISTINCT real_termination_reason_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'report_organization' AS base_name, COUNT(DISTINCT report_organization_tr) AS tr_distinct_count, COUNT(DISTINCT report_organization_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'ronesans_rank_personal' AS base_name, COUNT(DISTINCT ronesans_rank_personal_tr) AS tr_distinct_count, COUNT(DISTINCT ronesans_rank_personal_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'ronesans_rank' AS base_name, COUNT(DISTINCT ronesans_rank_tr) AS tr_distinct_count, COUNT(DISTINCT ronesans_rank_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'work_area' AS base_name, COUNT(DISTINCT work_area_tr) AS tr_distinct_count, COUNT(DISTINCT work_area_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;
SELECT 'workplace' AS base_name, COUNT(DISTINCT workplace_tr) AS tr_distinct_count, COUNT(DISTINCT workplace_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia;


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_newsf_eventreasons
-- =================================================================================

-- Missing Value Checks
SELECT 'name_tr' AS column_name, SUM(CASE WHEN name_tr IS NULL OR name_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_eventreasons;
SELECT 'name_en' AS column_name, SUM(CASE WHEN name_en IS NULL OR name_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_eventreasons;

-- Language Consistency Checks
SELECT 'name' AS base_name, COUNT(DISTINCT name_tr) AS tr_distinct_count, COUNT(DISTINCT name_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_eventreasons;


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_newsf_languages
-- =================================================================================

-- Missing Value Checks
SELECT 'language_tr' AS column_name, SUM(CASE WHEN language_tr IS NULL OR language_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_languages;
SELECT 'language_en' AS column_name, SUM(CASE WHEN language_en IS NULL OR language_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_languages;
SELECT 'writing_tr' AS column_name, SUM(CASE WHEN writing_tr IS NULL OR writing_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_languages;
SELECT 'writing_en' AS column_name, SUM(CASE WHEN writing_en IS NULL OR writing_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_languages;

-- Language Consistency Checks
SELECT 'language' AS base_name, COUNT(DISTINCT language_tr) AS tr_distinct_count, COUNT(DISTINCT language_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_languages;
SELECT 'writing' AS base_name, COUNT(DISTINCT writing_tr) AS tr_distinct_count, COUNT(DISTINCT writing_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_languages;


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_newsf_level_a
-- =================================================================================

-- Missing Value Checks
SELECT 'name_tr' AS column_name, SUM(CASE WHEN name_tr IS NULL OR name_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_a;
SELECT 'name_en' AS column_name, SUM(CASE WHEN name_en IS NULL OR name_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_a;

-- Language Consistency Checks
SELECT 'name' AS base_name, COUNT(DISTINCT name_tr) AS tr_distinct_count, COUNT(DISTINCT name_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_a;


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_newsf_level_b
-- =================================================================================

-- Missing Value Checks
SELECT 'name_tr' AS column_name, SUM(CASE WHEN name_tr IS NULL OR name_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_b;
SELECT 'name_en' AS column_name, SUM(CASE WHEN name_en IS NULL OR name_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_b;

-- Language Consistency Checks
SELECT 'name' AS base_name, COUNT(DISTINCT name_tr) AS tr_distinct_count, COUNT(DISTINCT name_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_b;


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_newsf_level_c
-- =================================================================================

-- Missing Value Checks
SELECT 'name_tr' AS column_name, SUM(CASE WHEN name_tr IS NULL OR name_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_c;
SELECT 'name_en' AS column_name, SUM(CASE WHEN name_en IS NULL OR name_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_c;

-- Language Consistency Checks
SELECT 'name' AS base_name, COUNT(DISTINCT name_tr) AS tr_distinct_count, COUNT(DISTINCT name_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_c;


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_newsf_level_d
-- =================================================================================

-- Missing Value Checks
SELECT 'name_tr' AS column_name, SUM(CASE WHEN name_tr IS NULL OR name_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_d;
SELECT 'name_en' AS column_name, SUM(CASE WHEN name_en IS NULL OR name_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_d;


-- Language Consistency Checks
SELECT 'name' AS base_name, COUNT(DISTINCT name_tr) AS tr_distinct_count, COUNT(DISTINCT name_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_d;


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_newsf_level_e
-- =================================================================================

-- Missing Value Checks
SELECT 'name_tr' AS column_name, SUM(CASE WHEN name_tr IS NULL OR name_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_e;
SELECT 'name_en' AS column_name, SUM(CASE WHEN name_en IS NULL OR name_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_e;

-- Language Consistency Checks
SELECT 'name' AS base_name, COUNT(DISTINCT name_tr) AS tr_distinct_count, COUNT(DISTINCT name_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_level_e;


-- =================================================================================
-- Table: raw__hr_kpi_t_sf_newsf_perperson
-- =================================================================================

-- Missing Value Checks
SELECT 'marital_status_tr' AS column_name, SUM(CASE WHEN marital_status_tr IS NULL OR marital_status_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_perperson;
SELECT 'marital_status_en' AS column_name, SUM(CASE WHEN marital_status_en IS NULL OR marital_status_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_perperson;
SELECT 'native_preferred_language_tr' AS column_name, SUM(CASE WHEN native_preferred_language_tr IS NULL OR native_preferred_language_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_perperson;
SELECT 'native_preferred_language_en' AS column_name, SUM(CASE WHEN native_preferred_language_en IS NULL OR native_preferred_language_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_perperson;
SELECT 'nationality_tr' AS column_name, SUM(CASE WHEN nationality_tr IS NULL OR nationality_tr = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_perperson;
SELECT 'nationality_en' AS column_name, SUM(CASE WHEN nationality_en IS NULL OR nationality_en = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_percentage FROM sf_odata.raw__hr_kpi_t_sf_newsf_perperson;

-- Language Consistency Checks
SELECT 'marital_status' AS base_name, COUNT(DISTINCT marital_status_tr) AS tr_distinct_count, COUNT(DISTINCT marital_status_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_perperson;
SELECT 'native_preferred_language' AS base_name, COUNT(DISTINCT native_preferred_language_tr) AS tr_distinct_count, COUNT(DISTINCT native_preferred_language_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_perperson;
SELECT 'nationality' AS base_name, COUNT(DISTINCT nationality_tr) AS tr_distinct_count, COUNT(DISTINCT nationality_en) AS en_distinct_count FROM sf_odata.raw__hr_kpi_t_sf_newsf_perperson;

