{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}

SELECT
  TOP 1048575 "source"."date" AS "date",
  "source"."reporting_date" AS "reporting_date",
  "source"."NOTS" AS "NOTS"
FROM
  (
    SELECT
      DISTINCT dt.date,
      vrl."reporting_date",
      'Bugün raw__powerbi_t_fact_viewreportlogs verisi güncellenmedi güncelleyiniz' NOTS
    FROM
      {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} dt
     
LEFT JOIN {{ source('stg_powerbi_kpi', 'raw__powerbi_t_fact_viewreportlogs') }} vrl ON dt.date = vrl.reporting_date
   
WHERE
      dt.date > DATEADD(DAY, -7, getdate())
      and dt.date < getdate()
      and vrl.reporting_date IS NULL

    UNION ALL

    SELECT
      DISTINCT dt.date,
      vrl."reporting_date2",
      'Bugün raw__s4hana_t_sap_reportusages verisi güncellenmedi güncelleyiniz'
    FROM
      {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} dt
      LEFT JOIN (
        SELECT
          *,
          cast(tarih as date) as reporting_date2
        FROM
            {{ source('stg_s4_odata', 'raw__s4hana_t_sap_reportusages') }}
        where
          IslemSayisi <> 0
      ) vrl ON dt.date = vrl.reporting_date2
    WHERE
      1 = 1
   AND dt.date > DATEADD(DAY, -7, getdate())
      and dt.date < DATEADD(DAY, -1, getdate())
      and vrl.reporting_date2 IS NULL
	UNION ALL
	SELECT
    DISTINCT dt.date,
    vrl."reporting_date2",
    'Bugün hr_kpi.raw__s4hana_t_sap_reportusages verisi güncellenmedi güncelleyiniz'
FROM
    {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} dt
    LEFT JOIN (
    SELECT
        *,
        cast(tarih as date) as reporting_date2
    FROM
        {{ source('stg_hr_kpi', 'raw__s4hana_t_sap_reportusages') }}
    where
        IslemSayisi <> 0
    ) vrl ON dt.date = vrl.reporting_date2
WHERE
    1 = 1
	AND dt.date > DATEADD(DAY, -7, getdate())
    and dt.date < DATEADD(DAY, -1, getdate())
    and vrl.reporting_date2 IS NULL

  ) "source"

UNION ALL

--stokozet kontrol
SELECT DISTINCT
	d.date
	,CAST(st.Tarih as date) db_upload_timestamp
	,'Bugün "raw__s4hana_t_sap_stokozetset" verisi güncellenmedi, güncelleyiniz.'
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				Tarih 
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_stokozetset') }}) st ON d.date = CAST(st.Tarih as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(st.Tarih as date) IS NULL

UNION ALL

--employeeassignitems kontrol
SELECT DISTINCT
	d.date
	,CAST(ei.Tarih as date) db_upload_timestamp
	,'Bugün "raw__s4hana_t_sap_employeeassignitems" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_employeeassignitems') }}) ei ON d.date = CAST(ei.tarih as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(ei.tarih as date) IS NULL

UNION ALL

--depo bazlý stok özet kontrol
SELECT DISTINCT
	d.date
	,CAST(dso.Tarih as date) db_upload_timestamp
	,'Bugün "raw__s4hana_t_sap_depobazlistokozet" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_depobazlistokozet') }} ) dso ON d.date = CAST(dso.Tarih as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(dso.Tarih as date) IS NULL

UNION ALL

--vehicle nodes kontrol
SELECT DISTINCT
	d.date
	,CAST(vn.db_upload_timestamp as date) db_upload_timestamp
	,'Bugün "[fms_kpi].[raw__fms_kpi_t_dim_vehiclenodes]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_dim_vehiclenodes') }}) vn ON d.date = CAST(vn.db_upload_timestamp as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(vn.db_upload_timestamp as date) IS NULL

UNION ALL

--fuel consumption report kontrol
SELECT DISTINCT
	d.date
	,CAST(fcr.EndDate as date) db_upload_timestamp
	,'Bugün "[fms_kpi].[raw__fms_kpi_t_fact_fuelconsumptionreport]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_fact_fuelconsumptionreport') }}) fcr ON d.date = CAST(fcr.EndDate as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(fcr.EndDate as date) IS NULL

UNION ALL

--idling duration report kontrol
SELECT DISTINCT
	d.date
	,CAST(idr.EndDate as date) db_upload_timestamp
	,'Bugün "[fms_kpi].[raw__fms_kpi_t_fact_idlingdurationreport]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_fact_idlingdurationreport') }}) idr ON d.date = CAST(idr.EndDate as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(idr.EndDate as date) IS NULL

UNION ALL

--ignition duration report kontrol
SELECT DISTINCT
	d.date
	,CAST(idr.EndDate as date) db_upload_timestamp
	,'Bugün "[fms_kpi].[raw__fms_kpi_t_fact_ignitiondurationreport]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_fact_ignitiondurationreport') }}) idr ON d.date = CAST(idr.EndDate as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(idr.EndDate as date) IS NULL

UNION ALL

--opet fuel expenses kontrol
SELECT DISTINCT
	d.date
	,CAST(ofe.db_upload_timestamp as date) db_upload_timestamp
	,'Bugün "[fms_kpi].[raw__fms_kpi_t_fact_opetfuelexpenses]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_fact_opetfuelexpenses') }}) ofe ON d.date = CAST(ofe.db_upload_timestamp as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(ofe.db_upload_timestamp as date) IS NULL

UNION ALL

--shell fuel expenses kontrol
SELECT DISTINCT
	d.date
	,CAST(sfe.Transaction_Date as date) db_upload_timestamp
	,'Bugün "[fms_kpi].[raw__fms_kpi_t_fact_shellfuelexpenses]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_fact_shellfuelexpenses') }}) sfe ON d.date = CAST(sfe.Transaction_Date as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(sfe.db_upload_timestamp as date) IS NULL

UNION ALL

--speed report kontrol
SELECT DISTINCT
	d.date
	,CAST(sr.EndDate as date) db_upload_timestamp
	,'Bugün "[fms_kpi].[raw__fms_kpi_t_fact_speedreport]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_fact_speedreport') }}) sr ON d.date = CAST(sr.EndDate as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(sr.EndDate as date) IS NULL

UNION ALL

--vehicle status kontrol
SELECT DISTINCT
	d.date
	,CAST(vs.reporting_date as date) reporting_date
	,'Bugün "[fms_kpi].[raw__fms_kpi_t_fact_vehiclestatus]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_fact_vehiclestatus') }}) vs ON d.date = CAST(vs.reporting_date as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(vs.reporting_date as date) IS NULL

UNION ALL 

--vehicles kontrol
SELECT DISTINCT
	d.date
	,CAST(v.db_upload_timestamp as date) db_upload_timestamp
	,'Bugün "[s4_odata].[raw__s4hana_t_sap_vehicles]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vehicles') }}) v ON d.date = CAST(v.db_upload_timestamp as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(v.db_upload_timestamp as date) IS NULL

UNION ALL

--vehicle debits kontrol
SELECT DISTINCT
	d.date
	,CAST(vd.db_upload_timestamp as date) db_upload_timestamp
	,'Bugün "[s4_odata].[raw__s4hana_t_sap_vehicledebits]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vehicledebits') }}) vd ON d.date = CAST(vd.db_upload_timestamp as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(vd.db_upload_timestamp as date) IS NULL

UNION ALL

--vehicle debits info kontrol
SELECT DISTINCT
	d.date
	,CAST(vdi.db_upload_timestamp as date) db_upload_timestamp
	,'Bugün "[s4_odata].[raw__s4hana_t_sap_vehicledebitsinfo]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vehicledebitsinfo') }}) vdi ON d.date = CAST(vdi.db_upload_timestamp as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(vdi.db_upload_timestamp  as date) IS NULL

UNION ALL

--reports kontrol
SELECT DISTINCT
	d.date
	,CAST(r.reporting_date as date) db_upload_timestamp
	,'Bugün "[powerbi].[raw__powerbi_t_dim_reports]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_powerbi_kpi', 'raw__powerbi_t_dim_reports') }} ) r ON d.date = CAST(r.reporting_date as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(r.reporting_date as date) IS NULL

UNION ALL

--report user kontrol
SELECT DISTINCT
	d.date
	,CAST(ru.reporting_date as date) db_upload_timestamp
	,'Bugün "[powerbi].[raw__powerbi_t_dim_reportusers]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_powerbi_kpi', 'raw__powerbi_t_dim_reportusers') }}) ru ON d.date = CAST(ru.reporting_date as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(ru.reporting_date as date) IS NULL

UNION ALL

--workspaces kontrol
SELECT DISTINCT
	d.date
	,CAST(ws.reporting_date as date) db_upload_timestamp
	,'Bugün "[powerbi].[raw__powerbi_t_dim_workspaces]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_powerbi_kpi', 'raw__powerbi_t_dim_workspaces') }}) ws ON d.date = CAST(ws.reporting_date as date)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CAST(ws.reporting_date as date) IS NULL

UNION ALL

--sap user log kontrol
SELECT DISTINCT
	d.date
	,CAST(sl.db_upload_timestamp as date) db_upload_timestamp
	,'Bugün "[s4_odata].[raw__s4hana_t_sap_sapuserlog]" verisi güncellenmedi, güncelleyiniz.' 
FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} d
LEFT JOIN (
			SELECT
				* 
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_sapuserlog') }}) sl ON d.date = CONVERT(date,sl.TransactionDate,104)
WHERE d.date <= DATEADD(DAY, -1, getdate())
	and d.date >= DATEADD(DAY, -7, getdate())
	and CONVERT(date,sl.TransactionDate,104) IS NULL