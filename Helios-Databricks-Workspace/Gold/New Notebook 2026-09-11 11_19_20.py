# Databricks notebook source
# MAGIC %sql
# MAGIC WITH device_inventory AS (
# MAGIC
# MAGIC     -- ============================================================
# MAGIC     -- SOLAR ASSETS
# MAGIC     -- ============================================================
# MAGIC
# MAGIC     SELECT
# MAGIC         site_id,
# MAGIC         asset_id AS device_id,
# MAGIC         'Solar' AS energy_type,
# MAGIC         status
# MAGIC     FROM helios.gold.solar_assets
# MAGIC
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC
# MAGIC     -- ============================================================
# MAGIC     -- WIND ASSETS
# MAGIC     -- ============================================================
# MAGIC
# MAGIC     SELECT
# MAGIC         site_id,
# MAGIC         asset_id AS device_id,
# MAGIC         'Wind' AS energy_type,
# MAGIC         status
# MAGIC     FROM helios.gold.wind_assets
# MAGIC
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC
# MAGIC     -- ============================================================
# MAGIC     -- SOLAR IoT DEVICES
# MAGIC     -- ============================================================
# MAGIC
# MAGIC     SELECT
# MAGIC         site_id,
# MAGIC         iot_id AS device_id,
# MAGIC         'Solar' AS energy_type,
# MAGIC         status
# MAGIC     FROM helios.gold.iot_devices
# MAGIC     WHERE UPPER(type) = 'SOLAR'
# MAGIC
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC
# MAGIC     -- ============================================================
# MAGIC     -- WIND IoT DEVICES
# MAGIC     -- ============================================================
# MAGIC
# MAGIC     SELECT
# MAGIC         site_id,
# MAGIC         iot_id AS device_id,
# MAGIC         'Wind' AS energy_type,
# MAGIC         status
# MAGIC     FROM helios.gold.iot_devices
# MAGIC     WHERE UPPER(type) = 'WIND'
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- COUNT DEVICES
# MAGIC -- ============================================================
# MAGIC
# MAGIC device_counts AS (
# MAGIC
# MAGIC     SELECT
# MAGIC
# MAGIC         site_id,
# MAGIC         energy_type,
# MAGIC
# MAGIC         COUNT(DISTINCT device_id) AS total_devices,
# MAGIC
# MAGIC         COUNT(
# MAGIC             DISTINCT CASE
# MAGIC                 WHEN status = TRUE
# MAGIC                 THEN device_id
# MAGIC             END
# MAGIC         ) AS active_devices,
# MAGIC
# MAGIC         COUNT(
# MAGIC             DISTINCT CASE
# MAGIC                 WHEN status = FALSE
# MAGIC                 THEN device_id
# MAGIC             END
# MAGIC         ) AS inactive_devices
# MAGIC
# MAGIC     FROM device_inventory
# MAGIC
# MAGIC     GROUP BY
# MAGIC         site_id,
# MAGIC         energy_type
# MAGIC )
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- ADD SITE INFORMATION
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     dc.site_id,
# MAGIC
# MAGIC     s.site_name,
# MAGIC     s.state,
# MAGIC     s.state_code,
# MAGIC
# MAGIC     dc.energy_type,
# MAGIC
# MAGIC     dc.total_devices,
# MAGIC     dc.active_devices,
# MAGIC     dc.inactive_devices,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN dc.total_devices > 0
# MAGIC         THEN ROUND(
# MAGIC             dc.active_devices * 100.0 / dc.total_devices,
# MAGIC             2
# MAGIC         )
# MAGIC         ELSE 0
# MAGIC     END AS active_percentage,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN dc.total_devices > 0
# MAGIC         THEN ROUND(
# MAGIC             dc.inactive_devices * 100.0 / dc.total_devices,
# MAGIC             2
# MAGIC         )
# MAGIC         ELSE 0
# MAGIC     END AS inactive_percentage
# MAGIC
# MAGIC FROM device_counts dc
# MAGIC
# MAGIC LEFT JOIN helios.gold.sites s
# MAGIC     ON dc.site_id = s.site_id;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW helios.gold.renewable_energy_view_test AS
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- SOLAR ENERGY
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     'Solar' AS energy_type,
# MAGIC
# MAGIC     s.site_id,
# MAGIC     st.site_name,
# MAGIC     st.state,
# MAGIC     st.state_code,
# MAGIC
# MAGIC     s.line_id AS asset_id,
# MAGIC     s.iot_device_id,
# MAGIC
# MAGIC     s.event_timestamp,
# MAGIC     s.power_generated,
# MAGIC
# MAGIC     -- Assuming power_generated is in Watts
# MAGIC     -- and each record represents 1 minute
# MAGIC     s.power_generated / 60.0 AS energy_generated_wh,
# MAGIC
# MAGIC     s.power_generated / 60000.0 AS energy_generated_kwh
# MAGIC
# MAGIC FROM helios.gold.solar_telemetry s
# MAGIC
# MAGIC LEFT JOIN helios.gold.sites st
# MAGIC     ON s.site_id = st.site_id
# MAGIC
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- WIND ENERGY
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     'Wind' AS energy_type,
# MAGIC
# MAGIC     w.site_id,
# MAGIC     st.site_name,
# MAGIC     st.state,
# MAGIC     st.state_code,
# MAGIC
# MAGIC     w.asset_id,
# MAGIC     w.iot_id AS iot_device_id,
# MAGIC
# MAGIC     w.event_timestamp,
# MAGIC     w.power_generated,
# MAGIC
# MAGIC     -- Assuming power_generated is in Watts
# MAGIC     -- and each record represents 1 minute
# MAGIC     w.power_generated / 60.0 AS energy_generated_wh,
# MAGIC
# MAGIC     w.power_generated / 60000.0 AS energy_generated_kwh
# MAGIC
# MAGIC FROM helios.gold.wind_telemetry w
# MAGIC
# MAGIC LEFT JOIN helios.gold.sites st
# MAGIC     ON w.site_id = st.site_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(power_generated) FROM helios.gold.renewable_energy_view_test WHERE energy_type = 'Solar'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(power_generated) FROM helios.gold.renewable_energy_view_test WHERE energy_type = 'Wind'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(power_generated) FROM helios.gold.solar_telemetry

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(power_generated) FROM helios.gold.wind_telemetry

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH device_inventory AS (
# MAGIC
# MAGIC     -- ============================================================
# MAGIC     -- SOLAR ASSETS
# MAGIC     -- ============================================================
# MAGIC
# MAGIC     SELECT
# MAGIC         site_id,
# MAGIC         asset_id AS device_id,
# MAGIC         'Solar' AS energy_type,
# MAGIC         status
# MAGIC     FROM helios.gold.solar_assets
# MAGIC
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC
# MAGIC     -- ============================================================
# MAGIC     -- WIND ASSETS
# MAGIC     -- ============================================================
# MAGIC
# MAGIC     SELECT
# MAGIC         site_id,
# MAGIC         asset_id AS device_id,
# MAGIC         'Wind' AS energy_type,
# MAGIC         status
# MAGIC     FROM helios.gold.wind_assets
# MAGIC
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC
# MAGIC     -- ============================================================
# MAGIC     -- SOLAR IoT DEVICES
# MAGIC     -- ============================================================
# MAGIC
# MAGIC     SELECT
# MAGIC         site_id,
# MAGIC         iot_id AS device_id,
# MAGIC         'Solar' AS energy_type,
# MAGIC         status
# MAGIC     FROM helios.gold.iot_devices
# MAGIC     WHERE UPPER(type) = 'SOLAR'
# MAGIC
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC
# MAGIC     -- ============================================================
# MAGIC     -- WIND IoT DEVICES
# MAGIC     -- ============================================================
# MAGIC
# MAGIC     SELECT
# MAGIC         site_id,
# MAGIC         iot_id AS device_id,
# MAGIC         'Wind' AS energy_type,
# MAGIC         status
# MAGIC     FROM helios.gold.iot_devices
# MAGIC     WHERE UPPER(type) = 'WIND'
# MAGIC
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC
# MAGIC     -- ============================================================
# MAGIC     -- WEATHER IoT DEVICES
# MAGIC     -- ============================================================
# MAGIC
# MAGIC     SELECT
# MAGIC         site_id,
# MAGIC         iot_id AS device_id,
# MAGIC         'Weather' AS energy_type,
# MAGIC         status
# MAGIC     FROM helios.gold.iot_devices
# MAGIC     WHERE UPPER(type) = 'WEATHER'
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- COUNT DEVICES
# MAGIC -- ============================================================
# MAGIC
# MAGIC device_counts AS (
# MAGIC
# MAGIC     SELECT
# MAGIC
# MAGIC         site_id,
# MAGIC         energy_type,
# MAGIC
# MAGIC         COUNT(DISTINCT device_id) AS total_devices,
# MAGIC
# MAGIC         COUNT(
# MAGIC             DISTINCT CASE
# MAGIC                 WHEN status = TRUE
# MAGIC                 THEN device_id
# MAGIC             END
# MAGIC         ) AS active_devices,
# MAGIC
# MAGIC         COUNT(
# MAGIC             DISTINCT CASE
# MAGIC                 WHEN status = FALSE
# MAGIC                 THEN device_id
# MAGIC             END
# MAGIC         ) AS inactive_devices
# MAGIC
# MAGIC     FROM device_inventory
# MAGIC
# MAGIC     GROUP BY
# MAGIC         site_id,
# MAGIC         energy_type
# MAGIC )
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- ADD SITE INFORMATION
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     dc.site_id,
# MAGIC
# MAGIC     s.site_name,
# MAGIC     s.state,
# MAGIC     s.state_code,
# MAGIC
# MAGIC     dc.energy_type,
# MAGIC
# MAGIC     dc.total_devices,
# MAGIC     dc.active_devices,
# MAGIC     dc.inactive_devices,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN dc.total_devices > 0
# MAGIC         THEN ROUND(
# MAGIC             dc.active_devices * 100.0 / dc.total_devices,
# MAGIC             2
# MAGIC         )
# MAGIC         ELSE 0
# MAGIC     END AS active_percentage,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN dc.total_devices > 0
# MAGIC         THEN ROUND(
# MAGIC             dc.inactive_devices * 100.0 / dc.total_devices,
# MAGIC             2
# MAGIC         )
# MAGIC         ELSE 0
# MAGIC     END AS inactive_percentage
# MAGIC
# MAGIC FROM device_counts dc
# MAGIC
# MAGIC LEFT JOIN helios.gold.sites s
# MAGIC     ON dc.site_id = s.site_id;