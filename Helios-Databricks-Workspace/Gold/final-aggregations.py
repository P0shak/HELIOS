# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW helios.gold.renewable_dashboard_view AS
# MAGIC
# MAGIC WITH
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- 1. DEVICE INVENTORY
# MAGIC -- ============================================================
# MAGIC -- Combine physical assets and IoT devices into one inventory.
# MAGIC -- This is used only to calculate device counts per site.
# MAGIC -- ============================================================
# MAGIC
# MAGIC device_inventory AS (
# MAGIC
# MAGIC     -- Solar physical assets
# MAGIC     SELECT
# MAGIC         site_id,
# MAGIC         asset_id AS device_id,
# MAGIC         'ASSET' AS device_type,
# MAGIC         status
# MAGIC     FROM helios.gold.solar_assets
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     -- Wind physical assets
# MAGIC     SELECT
# MAGIC         site_id,
# MAGIC         asset_id AS device_id,
# MAGIC         'ASSET' AS device_type,
# MAGIC         status
# MAGIC     FROM helios.gold.wind_assets
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     -- All IoTs: Solar, Wind and Weather
# MAGIC     SELECT
# MAGIC         site_id,
# MAGIC         iot_id AS device_id,
# MAGIC         'IOT' AS device_type,
# MAGIC         status
# MAGIC     FROM helios.gold.iot_devices
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- 2. DEVICE COUNTS PER SITE
# MAGIC -- ============================================================
# MAGIC
# MAGIC site_device_counts AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         site_id,
# MAGIC
# MAGIC         COUNT(DISTINCT device_id) AS total_devices,
# MAGIC
# MAGIC         COUNT(DISTINCT
# MAGIC             CASE
# MAGIC                 WHEN status = TRUE THEN device_id
# MAGIC             END
# MAGIC         ) AS total_active_devices,
# MAGIC
# MAGIC         COUNT(DISTINCT
# MAGIC             CASE
# MAGIC                 WHEN status = FALSE THEN device_id
# MAGIC             END
# MAGIC         ) AS total_inactive_devices
# MAGIC
# MAGIC     FROM device_inventory
# MAGIC
# MAGIC     GROUP BY site_id
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- 3. COMBINE SOLAR + WIND TELEMETRY
# MAGIC -- ============================================================
# MAGIC -- We combine them BEFORE assigning row numbers.
# MAGIC -- This is the important part.
# MAGIC --
# MAGIC -- Therefore:
# MAGIC --
# MAGIC -- AP01 Solar record 1  -> row_number = 1
# MAGIC -- AP01 Solar record 2  -> row_number = 2
# MAGIC -- AP01 Wind record 1   -> row_number = 3
# MAGIC -- AP01 Wind record 2   -> row_number = 4
# MAGIC --
# MAGIC -- Only row_number = 1 gets the device counts.
# MAGIC -- ============================================================
# MAGIC
# MAGIC combined_telemetry AS (
# MAGIC
# MAGIC     -- --------------------------------------------------------
# MAGIC     -- Solar telemetry
# MAGIC     -- --------------------------------------------------------
# MAGIC
# MAGIC     SELECT
# MAGIC         s.event_timestamp,
# MAGIC         s.site_id,
# MAGIC         s.company_name,
# MAGIC         'Solar' AS energy_type,
# MAGIC
# MAGIC         s.line_id,
# MAGIC         s.iot_device_id AS iot_id,
# MAGIC
# MAGIC         sa.asset_id AS asset_id,
# MAGIC
# MAGIC         'Solar' AS device_category,
# MAGIC
# MAGIC         sa.manufacturer_name AS asset_manufacturer,
# MAGIC         sa.mfg_date AS asset_mfg_date,
# MAGIC         sa.total_capacity_w AS asset_capacity_w,
# MAGIC         sa.status AS asset_status,
# MAGIC
# MAGIC         si.manufacturer_name AS iot_manufacturer,
# MAGIC         si.mfg_date AS iot_mfg_date,
# MAGIC         si.status AS iot_status,
# MAGIC         si.from_date AS iot_from_date,
# MAGIC         si.till_date AS iot_till_date,
# MAGIC
# MAGIC         s.number_of_panels,
# MAGIC         s.line_angle,
# MAGIC         s.panel_temperature AS temperature,
# MAGIC         s.power_generated,
# MAGIC
# MAGIC         CAST(NULL AS DOUBLE) AS wind_speed,
# MAGIC         CAST(NULL AS DOUBLE) AS vibration_intensity,
# MAGIC
# MAGIC         s.day_night,
# MAGIC         s.gen_id,
# MAGIC         s.created_at
# MAGIC
# MAGIC     FROM helios.gold.solar_telemetry s
# MAGIC
# MAGIC     LEFT JOIN helios.gold.solar_assets sa
# MAGIC         ON s.site_id = sa.site_id
# MAGIC         AND s.line_id = sa.line_id
# MAGIC
# MAGIC     LEFT JOIN helios.gold.iot_devices si
# MAGIC         ON s.iot_device_id = si.iot_id
# MAGIC         AND s.site_id = si.site_id
# MAGIC         AND UPPER(si.type) = 'SOLAR'
# MAGIC
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC
# MAGIC     -- --------------------------------------------------------
# MAGIC     -- Wind telemetry
# MAGIC     -- --------------------------------------------------------
# MAGIC
# MAGIC     SELECT
# MAGIC         w.event_timestamp,
# MAGIC         w.site_id,
# MAGIC         w.company_name,
# MAGIC         'Wind' AS energy_type,
# MAGIC
# MAGIC         CAST(NULL AS STRING) AS line_id,
# MAGIC         w.iot_id AS iot_id,
# MAGIC
# MAGIC         w.asset_id AS asset_id,
# MAGIC
# MAGIC         'Wind' AS device_category,
# MAGIC
# MAGIC         wa.manufacturer_name AS asset_manufacturer,
# MAGIC         wa.mfg_date AS asset_mfg_date,
# MAGIC         wa.total_capacity_w AS asset_capacity_w,
# MAGIC         wa.status AS asset_status,
# MAGIC
# MAGIC         wi.manufacturer_name AS iot_manufacturer,
# MAGIC         wi.mfg_date AS iot_mfg_date,
# MAGIC         wi.status AS iot_status,
# MAGIC         wi.from_date AS iot_from_date,
# MAGIC         wi.till_date AS iot_till_date,
# MAGIC
# MAGIC         CAST(NULL AS INT) AS number_of_panels,
# MAGIC         CAST(NULL AS DOUBLE) AS line_angle,
# MAGIC         CAST(NULL AS DOUBLE) AS temperature,
# MAGIC
# MAGIC         w.Power_generated AS power_generated,
# MAGIC
# MAGIC         w.windmill_speed AS wind_speed,
# MAGIC         w.vibration_intensity,
# MAGIC
# MAGIC         w.day_night,
# MAGIC         w.gen_id,
# MAGIC         w.created_at
# MAGIC
# MAGIC     FROM helios.gold.wind_telemetry w
# MAGIC
# MAGIC     LEFT JOIN helios.gold.wind_assets wa
# MAGIC         ON w.site_id = wa.site_id
# MAGIC         AND w.asset_id = wa.asset_id
# MAGIC
# MAGIC     LEFT JOIN helios.gold.iot_devices wi
# MAGIC         ON w.iot_id = wi.iot_id
# MAGIC         AND w.site_id = wi.site_id
# MAGIC         AND UPPER(wi.type) = 'WIND'
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- 4. ASSIGN ONE ROW NUMBER ACROSS BOTH SOLAR + WIND
# MAGIC -- ============================================================
# MAGIC -- This guarantees that a site gets its device count only once.
# MAGIC -- ============================================================
# MAGIC
# MAGIC numbered_telemetry AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         ct.*,
# MAGIC
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY ct.site_id
# MAGIC             ORDER BY
# MAGIC                 ct.event_timestamp,
# MAGIC                 ct.energy_type,
# MAGIC                 ct.gen_id
# MAGIC         ) AS site_row_number
# MAGIC
# MAGIC     FROM combined_telemetry ct
# MAGIC )
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- 5. FINAL DASHBOARD VIEW
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     nt.event_timestamp,
# MAGIC
# MAGIC     site.site_id,
# MAGIC     site.site_name,
# MAGIC     site.state,
# MAGIC     site.state_code,
# MAGIC     site.latitude,
# MAGIC     site.longitude,
# MAGIC
# MAGIC     nt.company_name,
# MAGIC     nt.energy_type,
# MAGIC
# MAGIC     nt.line_id,
# MAGIC     nt.iot_id,
# MAGIC     nt.asset_id,
# MAGIC
# MAGIC     nt.device_category,
# MAGIC
# MAGIC     nt.asset_manufacturer,
# MAGIC     nt.asset_mfg_date,
# MAGIC     nt.asset_capacity_w,
# MAGIC     nt.asset_status,
# MAGIC
# MAGIC     nt.iot_manufacturer,
# MAGIC     nt.iot_mfg_date,
# MAGIC     nt.iot_status,
# MAGIC     nt.iot_from_date,
# MAGIC     nt.iot_till_date,
# MAGIC
# MAGIC     -- ========================================================
# MAGIC     -- DEVICE COUNTS
# MAGIC     -- ========================================================
# MAGIC     -- Only ONE record per site contains the counts.
# MAGIC     -- Every other record contains 0.
# MAGIC     -- ========================================================
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN nt.site_row_number = 1
# MAGIC         THEN COALESCE(dc.total_devices, 0)
# MAGIC         ELSE 0
# MAGIC     END AS total_devices,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN nt.site_row_number = 1
# MAGIC         THEN COALESCE(dc.total_active_devices, 0)
# MAGIC         ELSE 0
# MAGIC     END AS total_active_devices,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN nt.site_row_number = 1
# MAGIC         THEN COALESCE(dc.total_inactive_devices, 0)
# MAGIC         ELSE 0
# MAGIC     END AS total_inactive_devices,
# MAGIC
# MAGIC     -- ========================================================
# MAGIC     -- TELEMETRY
# MAGIC     -- ========================================================
# MAGIC
# MAGIC     nt.number_of_panels,
# MAGIC     nt.line_angle,
# MAGIC     nt.temperature,
# MAGIC     nt.power_generated,
# MAGIC     nt.wind_speed,
# MAGIC     nt.vibration_intensity,
# MAGIC
# MAGIC     nt.day_night,
# MAGIC     nt.gen_id,
# MAGIC     nt.created_at
# MAGIC
# MAGIC FROM numbered_telemetry nt
# MAGIC
# MAGIC LEFT JOIN helios.gold.sites site
# MAGIC     ON nt.site_id = site.site_id
# MAGIC
# MAGIC LEFT JOIN site_device_counts dc
# MAGIC     ON nt.site_id = dc.site_id;

# COMMAND ----------

# %sql
# SELECT
#     SUM(total_devices) AS total_devices,
#     SUM(total_active_devices) AS total_active_devices,
#     SUM(total_inactive_devices) AS total_inactive_devices
# FROM helios.gold.renewable_dashboard_view;

# COMMAND ----------

# spark.sql("DESCRIBE TABLE helios.gold.solar_telemetry").show()

# COMMAND ----------

# spark.sql("DESCRIBE TABLE helios.gold.wind_telemetry").show()

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS helios.gold.solar_telemetry_alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW helios.gold.solar_telemetry_alerts AS
# MAGIC
# MAGIC WITH filtered_telemetry AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         line_id,
# MAGIC         iot_device_id,
# MAGIC         site_id,
# MAGIC         event_timestamp,
# MAGIC         panel_temperature,
# MAGIC         power_generated,
# MAGIC         line_angle,
# MAGIC         number_of_panels,
# MAGIC         day_night
# MAGIC
# MAGIC     FROM helios.gold.solar_telemetry
# MAGIC
# MAGIC     WHERE
# MAGIC         -- Warning temperature
# MAGIC         panel_temperature BETWEEN 60 AND 65
# MAGIC
# MAGIC         OR
# MAGIC
# MAGIC         -- Critical temperature
# MAGIC         panel_temperature > 65
# MAGIC
# MAGIC         OR
# MAGIC
# MAGIC         -- Warning power
# MAGIC         power_generated BETWEEN 15 AND 20
# MAGIC
# MAGIC         OR
# MAGIC
# MAGIC         -- Critical power
# MAGIC         power_generated < 15
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     -- =========================
# MAGIC     -- Telemetry identifiers
# MAGIC     -- =========================
# MAGIC     t.line_id,
# MAGIC     t.iot_device_id,
# MAGIC     t.site_id,
# MAGIC     t.event_timestamp,
# MAGIC
# MAGIC     -- =========================
# MAGIC     -- Solar telemetry
# MAGIC     -- =========================
# MAGIC     t.panel_temperature,
# MAGIC     t.power_generated,
# MAGIC     t.line_angle,
# MAGIC     t.number_of_panels,
# MAGIC     t.day_night,
# MAGIC
# MAGIC     -- =========================
# MAGIC     -- Site details
# MAGIC     -- =========================
# MAGIC     s.site_name,
# MAGIC     s.state,
# MAGIC     s.state_code,
# MAGIC     s.latitude,
# MAGIC     s.longitude,
# MAGIC     s.capacity_kw,
# MAGIC
# MAGIC     -- =========================
# MAGIC     -- Alert ID
# MAGIC     -- 3 = Warning
# MAGIC     -- 4 = Critical
# MAGIC     -- =========================
# MAGIC     CASE
# MAGIC
# MAGIC         -- Critical
# MAGIC         WHEN t.panel_temperature > 65
# MAGIC           OR t.power_generated < 15
# MAGIC         THEN 4
# MAGIC
# MAGIC         -- Warning
# MAGIC         WHEN t.panel_temperature BETWEEN 60 AND 65
# MAGIC           OR t.power_generated BETWEEN 15 AND 20
# MAGIC         THEN 3
# MAGIC
# MAGIC     END AS alert_id,
# MAGIC
# MAGIC     -- =========================
# MAGIC     -- Metric that caused alert
# MAGIC     -- =========================
# MAGIC     CASE
# MAGIC
# MAGIC         -- Both metrics are Critical
# MAGIC         WHEN t.panel_temperature > 65
# MAGIC          AND t.power_generated < 15
# MAGIC         THEN 'panel_temperature, power_generated'
# MAGIC
# MAGIC         -- Temperature is Critical
# MAGIC         WHEN t.panel_temperature > 65
# MAGIC         THEN 'panel_temperature'
# MAGIC
# MAGIC         -- Power is Critical
# MAGIC         WHEN t.power_generated < 15
# MAGIC         THEN 'power_generated'
# MAGIC
# MAGIC         -- Both metrics are Warning
# MAGIC         WHEN t.panel_temperature BETWEEN 60 AND 65
# MAGIC          AND t.power_generated BETWEEN 15 AND 20
# MAGIC         THEN 'panel_temperature, power_generated'
# MAGIC
# MAGIC         -- Temperature is Warning
# MAGIC         WHEN t.panel_temperature BETWEEN 60 AND 65
# MAGIC         THEN 'panel_temperature'
# MAGIC
# MAGIC         -- Power is Warning
# MAGIC         WHEN t.power_generated BETWEEN 15 AND 20
# MAGIC         THEN 'power_generated'
# MAGIC
# MAGIC     END AS alert_trigger_metric,
# MAGIC
# MAGIC     -- =========================
# MAGIC     -- Actual values that caused alert
# MAGIC     -- =========================
# MAGIC     CASE
# MAGIC
# MAGIC         -- Both metrics are Critical
# MAGIC         WHEN t.panel_temperature > 65
# MAGIC          AND t.power_generated < 15
# MAGIC         THEN CONCAT(
# MAGIC             'Temperature=',
# MAGIC             CAST(t.panel_temperature AS STRING),
# MAGIC             ' °C, Power=',
# MAGIC             CAST(t.power_generated AS STRING)
# MAGIC         )
# MAGIC
# MAGIC         -- Temperature is Critical
# MAGIC         WHEN t.panel_temperature > 65
# MAGIC         THEN CONCAT(
# MAGIC             'Temperature=',
# MAGIC             CAST(t.panel_temperature AS STRING),
# MAGIC             ' °C'
# MAGIC         )
# MAGIC
# MAGIC         -- Power is Critical
# MAGIC         WHEN t.power_generated < 15
# MAGIC         THEN CONCAT(
# MAGIC             'Power=',
# MAGIC             CAST(t.power_generated AS STRING)
# MAGIC         )
# MAGIC
# MAGIC         -- Both metrics are Warning
# MAGIC         WHEN t.panel_temperature BETWEEN 60 AND 65
# MAGIC          AND t.power_generated BETWEEN 15 AND 20
# MAGIC         THEN CONCAT(
# MAGIC             'Temperature=',
# MAGIC             CAST(t.panel_temperature AS STRING),
# MAGIC             ' °C, Power=',
# MAGIC             CAST(t.power_generated AS STRING)
# MAGIC         )
# MAGIC
# MAGIC         -- Temperature is Warning
# MAGIC         WHEN t.panel_temperature BETWEEN 60 AND 65
# MAGIC         THEN CONCAT(
# MAGIC             'Temperature=',
# MAGIC             CAST(t.panel_temperature AS STRING),
# MAGIC             ' °C'
# MAGIC         )
# MAGIC
# MAGIC         -- Power is Warning
# MAGIC         WHEN t.power_generated BETWEEN 15 AND 20
# MAGIC         THEN CONCAT(
# MAGIC             'Power=',
# MAGIC             CAST(t.power_generated AS STRING)
# MAGIC         )
# MAGIC
# MAGIC     END AS alert_trigger_value,
# MAGIC
# MAGIC     -- =========================
# MAGIC     -- Alert name and message
# MAGIC     -- =========================
# MAGIC     d.alert_name,
# MAGIC     d.message AS alert_message
# MAGIC
# MAGIC FROM filtered_telemetry t
# MAGIC
# MAGIC -- Join site after filtering
# MAGIC LEFT JOIN helios.gold.sites s
# MAGIC     ON t.site_id = s.site_id
# MAGIC
# MAGIC -- Join alert dimension
# MAGIC LEFT JOIN helios.gold.dim_alert_info d
# MAGIC     ON
# MAGIC         CASE
# MAGIC
# MAGIC             WHEN t.panel_temperature > 65
# MAGIC               OR t.power_generated < 15
# MAGIC             THEN 4
# MAGIC
# MAGIC             WHEN t.panel_temperature BETWEEN 60 AND 65
# MAGIC               OR t.power_generated BETWEEN 15 AND 20
# MAGIC             THEN 3
# MAGIC
# MAGIC         END = d.alert_id;

# COMMAND ----------

# %sql
# SELECT COUNT(*) FROM helios.gold.solar_telemetry_alerts

# COMMAND ----------

# %sql
# SELECT alert_name,COUNT(*) FROM helios.gold.solar_telemetry_alerts
# GROUP BY alert_name

# COMMAND ----------

# %sql
# SELECT alert_name, alert_trigger_value,COUNT(*) FROM helios.gold.solar_telemetry_alerts
# GROUP BY alert_name, alert_trigger_value

# COMMAND ----------

# %sql

# select power_generated, count(distinct power_generated) from helios.gold.solar_telemetry
#  GROUP BY power_generated order by power_generated 



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW helios.gold.wind_telemetry_alerts AS
# MAGIC
# MAGIC WITH filtered_telemetry AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         asset_id,
# MAGIC         iot_id,
# MAGIC         site_id,
# MAGIC         event_timestamp,
# MAGIC         windmill_speed,
# MAGIC         power_generated,
# MAGIC         vibration_intensity,
# MAGIC         heat_generated,
# MAGIC         day_night
# MAGIC
# MAGIC     FROM helios.gold.wind_telemetry
# MAGIC
# MAGIC     WHERE
# MAGIC         -- Wind Speed
# MAGIC         (windmill_speed > 39 AND windmill_speed <= 40)
# MAGIC         OR windmill_speed > 40
# MAGIC
# MAGIC         -- Power
# MAGIC         OR (power_generated >= 8 AND power_generated < 10)
# MAGIC         OR power_generated < 8
# MAGIC
# MAGIC         -- Vibration
# MAGIC         OR (vibration_intensity > 5.5 AND vibration_intensity <= 6)
# MAGIC         OR vibration_intensity > 6
# MAGIC
# MAGIC         -- Heat
# MAGIC         OR (heat_generated > 15000 AND heat_generated <= 17000)
# MAGIC         OR heat_generated > 17000
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     t.asset_id,
# MAGIC     t.iot_id,
# MAGIC     t.site_id,
# MAGIC     t.event_timestamp,
# MAGIC
# MAGIC     t.windmill_speed,
# MAGIC     t.power_generated,
# MAGIC     t.vibration_intensity,
# MAGIC     t.heat_generated,
# MAGIC     t.day_night,
# MAGIC
# MAGIC     s.site_name,
# MAGIC     s.state,
# MAGIC     s.state_code,
# MAGIC     s.latitude,
# MAGIC     s.longitude,
# MAGIC     s.capacity_kw,
# MAGIC
# MAGIC     -- Alert ID
# MAGIC     CASE
# MAGIC         -- Critical
# MAGIC         WHEN t.windmill_speed > 40
# MAGIC           OR t.power_generated < 8
# MAGIC           OR t.vibration_intensity > 6
# MAGIC           OR t.heat_generated > 17000
# MAGIC         THEN 4
# MAGIC
# MAGIC         -- Warning
# MAGIC         WHEN (t.windmill_speed > 39
# MAGIC               AND t.windmill_speed <= 40)
# MAGIC           OR (t.power_generated >= 8
# MAGIC               AND t.power_generated < 10)
# MAGIC           OR (t.vibration_intensity > 5.5
# MAGIC               AND t.vibration_intensity <= 6)
# MAGIC           OR (t.heat_generated > 15000
# MAGIC               AND t.heat_generated <= 17000)
# MAGIC         THEN 3
# MAGIC     END AS alert_id,
# MAGIC
# MAGIC     -- Alert Trigger Metric
# MAGIC     CASE
# MAGIC         WHEN t.windmill_speed > 40
# MAGIC         THEN 'Wind Speed'
# MAGIC
# MAGIC         WHEN t.power_generated < 8
# MAGIC         THEN 'Power'
# MAGIC
# MAGIC         WHEN t.vibration_intensity > 6
# MAGIC         THEN 'Vibration'
# MAGIC
# MAGIC         WHEN t.heat_generated > 17000
# MAGIC         THEN 'Heat'
# MAGIC
# MAGIC         WHEN t.windmill_speed > 39
# MAGIC              AND t.windmill_speed <= 40
# MAGIC         THEN 'Wind Speed'
# MAGIC
# MAGIC         WHEN t.power_generated >= 8
# MAGIC              AND t.power_generated < 10
# MAGIC         THEN 'Power'
# MAGIC
# MAGIC         WHEN t.vibration_intensity > 5.5
# MAGIC              AND t.vibration_intensity <= 6
# MAGIC         THEN 'Vibration'
# MAGIC
# MAGIC         WHEN t.heat_generated > 15000
# MAGIC              AND t.heat_generated <= 17000
# MAGIC         THEN 'Heat'
# MAGIC     END AS alert_trigger_metric,
# MAGIC
# MAGIC     -- Alert Trigger Value
# MAGIC     CASE
# MAGIC         WHEN t.windmill_speed > 40
# MAGIC         THEN CONCAT('Wind Speed = ', ROUND(t.windmill_speed, 2))
# MAGIC
# MAGIC         WHEN t.power_generated < 8
# MAGIC         THEN CONCAT('Power = ', ROUND(t.power_generated, 2))
# MAGIC
# MAGIC         WHEN t.vibration_intensity > 6
# MAGIC         THEN CONCAT('Vibration = ', ROUND(t.vibration_intensity, 2))
# MAGIC
# MAGIC         WHEN t.heat_generated > 17000
# MAGIC         THEN CONCAT('Heat = ', ROUND(t.heat_generated, 2))
# MAGIC
# MAGIC         WHEN t.windmill_speed > 39
# MAGIC              AND t.windmill_speed <= 40
# MAGIC         THEN CONCAT('Wind Speed = ', ROUND(t.windmill_speed, 2))
# MAGIC
# MAGIC         WHEN t.power_generated >= 8
# MAGIC              AND t.power_generated < 10
# MAGIC         THEN CONCAT('Power = ', ROUND(t.power_generated, 2))
# MAGIC
# MAGIC         WHEN t.vibration_intensity > 5.5
# MAGIC              AND t.vibration_intensity <= 6
# MAGIC         THEN CONCAT('Vibration = ', ROUND(t.vibration_intensity, 2))
# MAGIC
# MAGIC         WHEN t.heat_generated > 15000
# MAGIC              AND t.heat_generated <= 17000
# MAGIC         THEN CONCAT('Heat = ', ROUND(t.heat_generated, 2))
# MAGIC     END AS alert_trigger_value,
# MAGIC
# MAGIC     d.alert_name,
# MAGIC     d.message AS alert_message
# MAGIC
# MAGIC FROM filtered_telemetry t
# MAGIC
# MAGIC LEFT JOIN helios.gold.sites s
# MAGIC     ON t.site_id = s.site_id
# MAGIC
# MAGIC LEFT JOIN helios.gold.dim_alert_info d
# MAGIC     ON (
# MAGIC         CASE
# MAGIC             WHEN t.windmill_speed > 40
# MAGIC               OR t.power_generated < 8
# MAGIC               OR t.vibration_intensity > 6
# MAGIC               OR t.heat_generated > 17000
# MAGIC             THEN 4
# MAGIC
# MAGIC             WHEN (t.windmill_speed > 39
# MAGIC                   AND t.windmill_speed <= 40)
# MAGIC               OR (t.power_generated >= 8
# MAGIC                   AND t.power_generated < 10)
# MAGIC               OR (t.vibration_intensity > 5.5
# MAGIC                   AND t.vibration_intensity <= 6)
# MAGIC               OR (t.heat_generated > 15000
# MAGIC                   AND t.heat_generated <= 17000)
# MAGIC             THEN 3
# MAGIC         END
# MAGIC     ) = d.alert_id;

# COMMAND ----------

# %sql
# SELECT alert_name,COUNT(*) FROM helios.gold.wind_telemetry_alerts
# GROUP BY alert_name

# COMMAND ----------

# %sql
# SELECT alert_name,COUNT(*),alert_trigger_value FROM helios.gold.wind_telemetry_alerts
# GROUP BY alert_name,alert_trigger_value

# COMMAND ----------

# %sql
# SELECT * FROM helios.gold.solar_telemetry_alerts ORDER BY event_timestamp DESC LIMIT 10

# COMMAND ----------

# %sql
# SELECT * FROM helios.gold.wind_telemetry_alerts ORDER BY event_timestamp LIMIT 10