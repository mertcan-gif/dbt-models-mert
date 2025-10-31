Successfully loaded metadata for 131 columns from 'data/COLUMNS_202510131556.csv'.

Connection to database established successfully!
────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_job_application_education_newsf_rmore
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_job_application_education_newsf_rmore...
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
Data loaded successfully with 8656 rows.

--- Checking for Null or Empty Values ---
  - Column 'country_tr': OK (0 null/empty values)
  - Column 'country_en': OK (0 null/empty values)
  - Column 'graduation_tr': OK (0 null/empty values)
  - Column 'graduation_en': OK (0 null/empty values)
  - Column 'name_of_school_tr': OK (0 null/empty values)
  - Column 'name_of_school_en': OK (0 null/empty values)

--- Checking for Language Mismatches (Distinct Counts) ---
  - OK for 'country': TR and EN counts are both 47
  - MISMATCH in 'graduation': TR count = 2, EN count = 4
  - MISMATCH in 'name_of_school': TR count = 274, EN count = 275


────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_job_application_languages_newsf_rmore
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_job_application_languages_newsf_rmore...
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
Data loaded successfully with 6634 rows.

--- Checking for Null or Empty Values ---
  - Column 'language_tr': OK (0 null/empty values)
  - Column 'language_en': OK (0 null/empty values)
  - Column 'writing_tr': OK (0 null/empty values)
  - Column 'writing_en': OK (0 null/empty values)
  - Column 'reading_tr': OK (0 null/empty values)
  - Column 'reading_en': OK (0 null/empty values)
  - Column 'speaking_tr': OK (0 null/empty values)
  - Column 'speaking_en': OK (0 null/empty values)

--- Checking for Language Mismatches (Distinct Counts) ---
  - OK for 'language': TR and EN counts are both 41
  - OK for 'reading': TR and EN counts are both 5
  - OK for 'speaking': TR and EN counts are both 5
  - OK for 'writing': TR and EN counts are both 5


────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_job_applications_newsf_rmore
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_job_applications_newsf_rmore...
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
Data loaded successfully with 19878 rows.

--- Checking for Null or Empty Values ---
  - Column 'teklif_retnedenleri_tr': OK (0 null/empty values)
  - Column 'teklif_retnedenleri_en': OK (0 null/empty values)
  - Column 'onr_departman_tr': OK (0 null/empty values)
  - Column 'onr_departman_en': OK (0 null/empty values)
  - Column 'oneren_yetkili_tr': OK (0 null/empty values)
  - Column 'oneren_yetkili_en': OK (0 null/empty values)
  - Column 'gorusen_kisi_ik_tr': OK (0 null/empty values)
  - Column 'gorusen_kisi_ik_en': OK (0 null/empty values)
  - Column 'gorusen_kisi_ik_ygf_tr': OK (0 null/empty values)
  - Column 'gorusen_kisi_ik_ygf_en': OK (0 null/empty values)
  - Column 'gorusen_kisi_ik_ygf_iki_tr': OK (0 null/empty values)
  - Column 'gorusen_kisi_ik_ygf_iki_en': OK (0 null/empty values)
  - Column 'gorusen_kisi_ik_ygf_uc_tr': OK (0 null/empty values)
  - Column 'gorusen_kisi_ik_ygf_uc_en': OK (0 null/empty values)
  - Column 'durum_tr': OK (0 null/empty values)
  - Column 'durum_en': OK (0 null/empty values)
  - Column 'uyruk_tr': OK (0 null/empty values)
  - Column 'uyruk_en': OK (0 null/empty values)
  - Column 'engellilik_durumu_tr': OK (0 null/empty values)
  - Column 'engellilik_durumu_en': OK (0 null/empty values)
  - Column 'askerlik_durumu_tr': OK (0 null/empty values)
  - Column 'askerlik_durumu_en': OK (0 null/empty values)
...
────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_newsf_employee_assign_items
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_newsf_employee_assign_items...
Output is truncated. View as a scrollable element or open in a text editor. Adjust cell output settings...
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
Data loaded successfully with 12490 rows.

--- Checking for Null or Empty Values ---
  - Column 'assigned_item_name_tr': OK (0 null/empty values)

--- Checking for Language Mismatches (Distinct Counts) ---
  No English/Turkish column pairs found for this table in the metadata file.


────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_newsf_employee_education
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_newsf_employee_education...
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
Data loaded successfully with 3618 rows.

--- Checking for Null or Empty Values ---
  - Column 'country_tr': OK (0 null/empty values)
  - Column 'country_en': OK (0 null/empty values)
  - Column 'graduation_tr': OK (0 null/empty values)
  - Column 'graduation_en': OK (0 null/empty values)
  - Column 'level_of_education_name_tr': OK (0 null/empty values)
  - Column 'level_of_education_name_en': OK (0 null/empty values)
  - Column 'name_of_school_tr': OK (0 null/empty values)
  - Column 'name_of_school_en': OK (0 null/empty values)
  - Column 'field_of_study_tr': OK (0 null/empty values)
  - Column 'field_of_study_en': OK (0 null/empty values)

--- Checking for Language Mismatches (Distinct Counts) ---
  - OK for 'country': TR and EN counts are both 36
  - MISMATCH in 'field_of_study': TR count = 325, EN count = 328
  - MISMATCH in 'graduation': TR count = 2, EN count = 4
  - OK for 'level_of_education_name': TR and EN counts are both 6
  - MISMATCH in 'name_of_school': TR count = 231, EN count = 232


────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_newsf_employees
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_newsf_employees...
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
Data loaded successfully with 7838 rows.

--- Checking for Null or Empty Values ---
  - Column 'employee_status_tr': OK (0 null/empty values)
  - Column 'employee_status_en': OK (0 null/empty values)
  - Column 'ronesans_rank_tr': OK (0 null/empty values)
  - Column 'ronesans_rank_en': OK (0 null/empty values)
  - Column 'ronesans_rank_personal_tr': OK (0 null/empty values)
  - Column 'ronesans_rank_personal_en': OK (0 null/empty values)
  - Column 'workplace_tr': OK (0 null/empty values)
  - Column 'workplace_en': OK (0 null/empty values)
  - Column 'employee_area_en': OK (0 null/empty values)
  - Column 'employee_group_tr': OK (0 null/empty values)
  - Column 'employee_group_en': OK (0 null/empty values)
  - Column 'employee_sub_group_tr': OK (0 null/empty values)
  - Column 'employee_sub_group_en': OK (0 null/empty values)
  - Column 'work_area_tr': OK (0 null/empty values)
  - Column 'work_area_en': OK (0 null/empty values)
  - Column 'real_termination_reason_tr': OK (0 null/empty values)
  - Column 'real_termination_reason_en': OK (0 null/empty values)
  - Column 'marital_status_name_tr': OK (0 null/empty values)
  - Column 'marital_status_name_en': OK (0 null/empty values)
  - Column 'preferred_language_tr': OK (0 null/empty values)
  - Column 'preferred_language_en': OK (0 null/empty values)
  - Column 'employee_type_name_tr': OK (0 null/empty values)
...
────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_newsf_employees_historia
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_newsf_employees_historia...
Output is truncated. View as a scrollable element or open in a text editor. Adjust cell output settings...
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
Data loaded successfully with 19358 rows.

--- Checking for Null or Empty Values ---
  - Column 'employee_status_tr': OK (0 null/empty values)
  - Column 'employee_status_en': OK (0 null/empty values)
  - Column 'ronesans_rank_tr': OK (0 null/empty values)
  - Column 'ronesans_rank_en': OK (0 null/empty values)
  - Column 'ronesans_rank_personal_tr': OK (0 null/empty values)
  - Column 'ronesans_rank_personal_en': OK (0 null/empty values)
  - Column 'workplace_tr': OK (0 null/empty values)
  - Column 'workplace_en': OK (0 null/empty values)
  - Column 'employee_area_en': OK (0 null/empty values)
  - Column 'employee_group_tr': OK (0 null/empty values)
  - Column 'employee_group_en': OK (0 null/empty values)
  - Column 'employee_sub_group_tr': OK (0 null/empty values)
  - Column 'employee_sub_group_en': OK (0 null/empty values)
  - Column 'work_area_tr': OK (0 null/empty values)
  - Column 'work_area_en': OK (0 null/empty values)
  - Column 'real_termination_reason_tr': OK (0 null/empty values)
  - Column 'real_termination_reason_en': OK (0 null/empty values)
  - Column 'marital_status_name_tr': OK (0 null/empty values)
  - Column 'marital_status_name_en': OK (0 null/empty values)
  - Column 'preferred_language_tr': OK (0 null/empty values)
  - Column 'preferred_language_en': OK (0 null/empty values)
  - Column 'employee_type_name_tr': OK (0 null/empty values)
...
────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_newsf_eventreasons
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_newsf_eventreasons...
Output is truncated. View as a scrollable element or open in a text editor. Adjust cell output settings...
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
Data loaded successfully with 81 rows.

--- Checking for Null or Empty Values ---
  - Column 'name_tr': 16 null/empty values (19.75%)
  - Column 'name_en': 1 null/empty values (1.23%)

--- Checking for Language Mismatches (Distinct Counts) ---
  - MISMATCH in 'name': TR count = 66, EN count = 80


────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_newsf_languages
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_newsf_languages...
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
Data loaded successfully with 2724 rows.

--- Checking for Null or Empty Values ---
  - Column 'language_tr': OK (0 null/empty values)
  - Column 'language_en': OK (0 null/empty values)
  - Column 'writing_tr': OK (0 null/empty values)
  - Column 'writing_en': OK (0 null/empty values)
  - Column 'reading_tr': OK (0 null/empty values)
  - Column 'reading_en': OK (0 null/empty values)
  - Column 'speaking_tr': OK (0 null/empty values)
  - Column 'speaking_en': OK (0 null/empty values)

--- Checking for Language Mismatches (Distinct Counts) ---
  - OK for 'language': TR and EN counts are both 38
  - OK for 'reading': TR and EN counts are both 5
  - OK for 'speaking': TR and EN counts are both 5
  - OK for 'writing': TR and EN counts are both 5


────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_newsf_level_a
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_newsf_level_a...
Data loaded successfully with 25 rows.
...
────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_newsf_level_b
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_newsf_level_b...
Output is truncated. View as a scrollable element or open in a text editor. Adjust cell output settings...
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
Data loaded successfully with 148 rows.

--- Checking for Null or Empty Values ---
  - Column 'name_tr': OK (0 null/empty values)
  - Column 'name_en': 4 null/empty values (2.70%)

--- Checking for Language Mismatches (Distinct Counts) ---
  - MISMATCH in 'name': TR count = 104, EN count = 105


────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_newsf_level_c
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_newsf_level_c...
Data loaded successfully with 626 rows.

--- Checking for Null or Empty Values ---
  - Column 'name_tr': OK (0 null/empty values)
  - Column 'name_en': 28 null/empty values (4.47%)

--- Checking for Language Mismatches (Distinct Counts) ---
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
  - MISMATCH in 'name': TR count = 241, EN count = 252


────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_newsf_level_d
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_newsf_level_d...
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
Data loaded successfully with 634 rows.

--- Checking for Null or Empty Values ---
  - Column 'name_tr': 2 null/empty values (0.32%)
  - Column 'name_en': 45 null/empty values (7.10%)

--- Checking for Language Mismatches (Distinct Counts) ---
  - MISMATCH in 'name': TR count = 310, EN count = 326


────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_newsf_level_e
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_newsf_level_e...
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
Data loaded successfully with 1165 rows.

--- Checking for Null or Empty Values ---
  - Column 'name_tr': 8 null/empty values (0.69%)
  - Column 'name_en': 65 null/empty values (5.58%)

--- Checking for Language Mismatches (Distinct Counts) ---
  - MISMATCH in 'name': TR count = 282, EN count = 279


────────────────────────────────────────────────────────────────────────────────
Processing Table: sf_odata.raw__hr_kpi_t_sf_newsf_perperson
────────────────────────────────────────────────────────────────────────────────
Loading data from raw__hr_kpi_t_sf_newsf_perperson...
C:\Users\mertc\AppData\Local\Temp\ipykernel_15596\3224062640.py:56: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df = pd.read_sql(query, cnxn)
Data loaded successfully with 9854 rows.

--- Checking for Null or Empty Values ---
  - Column 'marital_status_tr': OK (0 null/empty values)
  - Column 'marital_status_en': OK (0 null/empty values)
  - Column 'native_preferred_language_tr': OK (0 null/empty values)
  - Column 'native_preferred_language_en': OK (0 null/empty values)

--- Checking for Language Mismatches (Distinct Counts) ---
  - OK for 'marital_status': TR and EN counts are both 3
  - OK for 'native_preferred_language': TR and EN counts are both 12

