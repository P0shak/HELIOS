# Databricks notebook source
# MAGIC %sql
# MAGIC select count(*) from helios.bronze.ap_solar_telemetry;
# MAGIC select * from helios.bronze.tn_solar_telemetry;
# MAGIC select * from helios.bronze.wa_solar_telemetry;
# MAGIC select * from helios.bronze.or_solar_telemetry;
# MAGIC select * from helios.bronze.nv_solar_telemetry;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from helios.silver.ap_solar_telemetry;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from helios.silver.kl_solar_telemetry where 1 =1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables in helios.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use catalog helios;
# MAGIC
# MAGIC SELECT
# MAGIC     'ap_solar_telemetry' AS table_name,
# MAGIC     (SELECT COUNT(*) FROM bronze.ap_solar_telemetry) AS bronze_count,
# MAGIC     (SELECT COUNT(*) FROM silver.ap_solar_telemetry) AS silver_count
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'ap_wind_telemetry',
# MAGIC     (SELECT COUNT(*) FROM bronze.ap_wind_telemetry),
# MAGIC     (SELECT COUNT(*) FROM silver.ap_wind_telemetry)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'kl_solar_telemetry',
# MAGIC     (SELECT COUNT(*) FROM bronze.kl_solar_telemetry),
# MAGIC     (SELECT COUNT(*) FROM silver.kl_solar_telemetry)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'kl_wind_telemetry',
# MAGIC     (SELECT COUNT(*) FROM bronze.kl_wind_telemetry),
# MAGIC     (SELECT COUNT(*) FROM silver.kl_wind_telemetry)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'ks_solar_telemetry',
# MAGIC     (SELECT COUNT(*) FROM bronze.ks_solar_telemetry),
# MAGIC     (SELECT COUNT(*) FROM silver.ks_solar_telemetry)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'ks_wind_telemetry',
# MAGIC     (SELECT COUNT(*) FROM bronze.ks_wind_telemetry),
# MAGIC     (SELECT COUNT(*) FROM silver.ks_wind_telemetry)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'tn_solar_telemetry',
# MAGIC     (SELECT COUNT(*) FROM bronze.tn_solar_telemetry),
# MAGIC     (SELECT COUNT(*) FROM silver.tn_solar_telemetry)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'tn_wind_telemetry',
# MAGIC     (SELECT COUNT(*) FROM bronze.tn_wind_telemetry),
# MAGIC     (SELECT COUNT(*) FROM silver.tn_wind_telemetry)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'ts_solar_telemetry',
# MAGIC     (SELECT COUNT(*) FROM bronze.ts_solar_telemetry),
# MAGIC     (SELECT COUNT(*) FROM silver.ts_solar_telemetry)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'ts_wind_telemetry',
# MAGIC     (SELECT COUNT(*) FROM bronze.ts_wind_telemetry),
# MAGIC     (SELECT COUNT(*) FROM silver.ts_wind_telemetry);

# COMMAND ----------

df = spark.sql("select * from helios.silver.ap_wind_telemetry")

df.printSchema()

#df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from helios.silver.kl_wind_telemetry limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --select * from helios.gold.wind_telemetry

# COMMAND ----------

# df = spark.sql("select * from helios.gold.wind_telemetry")

df = spark.sql("select * from helios.bronze.kl_solar_telemetry where power_generated>30")


#df.printSchema()

df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC # WIND INSERT STATEMENTS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- WIND_AP_01
# MAGIC -- SequenceNumber: 80000008051 - 80000008060
# MAGIC -- ============================================================
# MAGIC
# MAGIC INSERT INTO helios.bronze.ap_wind_telemetry
# MAGIC (
# MAGIC     SequenceNumber,
# MAGIC     Offset,
# MAGIC     EnqueuedTimeUtc,
# MAGIC     asset_id,
# MAGIC     iot_id,
# MAGIC     site_id,
# MAGIC     company_name,
# MAGIC     windmill_speed,
# MAGIC     power_generated,
# MAGIC     vibration_intensity,
# MAGIC     heat_generated,
# MAGIC     event_timestamp,
# MAGIC     day_night,
# MAGIC     timestamp
# MAGIC )
# MAGIC VALUES
# MAGIC (80000008051, 90000008051, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_01', 'IOT_WIND_AP_01', 'AP01', 'Sagar Energy',
# MAGIC  14.82, 48.35, 1.12, 4325.60,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008052, 90000008052, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_01', 'IOT_WIND_AP_01', 'AP01', 'Sagar Energy',
# MAGIC  22.45, 65.72, 1.86, 6240.25,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008053, 90000008053, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_01', 'IOT_WIND_AP_01', 'AP01', 'Sagar Energy',
# MAGIC  29.38, 78.45, 2.42, 7825.40,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008054, 90000008054, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_01', 'IOT_WIND_AP_01', 'AP01', 'Sagar Energy',
# MAGIC  32.74, 71.36, 2.68, 8450.72,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008055, 90000008055, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_01', 'IOT_WIND_AP_01', 'AP01', 'Sagar Energy',
# MAGIC  33.42, 22.84, 3.12, 9675.34,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008056, 90000008056, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_01', 'IOT_WIND_AP_01', 'AP01', 'Sagar Energy',
# MAGIC  34.68, 18.76, 3.46, 11380.25,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008057, 90000008057, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_01', 'IOT_WIND_AP_01', 'AP01', 'Sagar Energy',
# MAGIC  35.42, 14.82, 3.72, 12140.68,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008058, 90000008058, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_01', 'IOT_WIND_AP_01', 'AP01', 'Sagar Energy',
# MAGIC  36.18, 12.64, 4.18, 12875.45,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008059, 90000008059, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_01', 'IOT_WIND_AP_01', 'AP01', 'Sagar Energy',
# MAGIC  37.26, 9.42, 4.76, 13780.82,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008060, 90000008060, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_01', 'IOT_WIND_AP_01', 'AP01', 'Sagar Energy',
# MAGIC  38.64, 7.85, 5.24, 14625.90,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30');
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- WIND_AP_02
# MAGIC -- SequenceNumber: 80000008061 - 80000008070
# MAGIC -- ============================================================
# MAGIC
# MAGIC INSERT INTO helios.bronze.ap_wind_telemetry
# MAGIC (
# MAGIC     SequenceNumber,
# MAGIC     Offset,
# MAGIC     EnqueuedTimeUtc,
# MAGIC     asset_id,
# MAGIC     iot_id,
# MAGIC     site_id,
# MAGIC     company_name,
# MAGIC     windmill_speed,
# MAGIC     power_generated,
# MAGIC     vibration_intensity,
# MAGIC     heat_generated,
# MAGIC     event_timestamp,
# MAGIC     day_night,
# MAGIC     timestamp
# MAGIC )
# MAGIC VALUES
# MAGIC (80000008061, 90000008061, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_02', 'IOT_WIND_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  12.76, 42.52, 0.94, 3820.45,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008062, 90000008062, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_02', 'IOT_WIND_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  21.38, 61.84, 1.72, 5845.62,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008063, 90000008063, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_02', 'IOT_WIND_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  30.42, 79.26, 2.38, 7650.75,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008064, 90000008064, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_02', 'IOT_WIND_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  32.86, 24.68, 2.76, 8245.36,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008065, 90000008065, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_02', 'IOT_WIND_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  33.58, 21.42, 3.18, 9460.72,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008066, 90000008066, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_02', 'IOT_WIND_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  34.74, 17.85, 3.42, 10875.43,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008067, 90000008067, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_02', 'IOT_WIND_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  35.38, 14.36, 3.68, 11940.25,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008068, 90000008068, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_02', 'IOT_WIND_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  36.24, 11.72, 4.06, 12680.58,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008069, 90000008069, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_02', 'IOT_WIND_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  37.56, 8.64, 4.72, 13845.67,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008070, 90000008070, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_AP_02', 'IOT_WIND_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  38.82, 6.95, 5.36, 15120.84,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30');
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- WIND_KL_04
# MAGIC -- SequenceNumber: 80000008071 - 80000008080
# MAGIC -- ============================================================
# MAGIC
# MAGIC INSERT INTO helios.bronze.kl_wind_telemetry
# MAGIC (
# MAGIC     SequenceNumber,
# MAGIC     Offset,
# MAGIC     EnqueuedTimeUtc,
# MAGIC     asset_id,
# MAGIC     iot_id,
# MAGIC     site_id,
# MAGIC     company_name,
# MAGIC     windmill_speed,
# MAGIC     power_generated,
# MAGIC     vibration_intensity,
# MAGIC     heat_generated,
# MAGIC     event_timestamp,
# MAGIC     day_night,
# MAGIC     timestamp
# MAGIC )
# MAGIC VALUES
# MAGIC (80000008071, 90000008071, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_04', 'IOT_WIND_KL_04', 'KL01', 'Sagar Energy',
# MAGIC  13.42, 45.76, 1.05, 4015.32,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008072, 90000008072, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_04', 'IOT_WIND_KL_04', 'KL01', 'Sagar Energy',
# MAGIC  23.18, 68.91, 1.86, 6125.25,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008073, 90000008073, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_04', 'IOT_WIND_KL_04', 'KL01', 'Sagar Energy',
# MAGIC  31.24, 81.14, 2.44, 7890.72,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008074, 90000008074, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_04', 'IOT_WIND_KL_04', 'KL01', 'Sagar Energy',
# MAGIC  32.64, 23.76, 2.82, 8565.38,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008075, 90000008075, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_04', 'IOT_WIND_KL_04', 'KL01', 'Sagar Energy',
# MAGIC  33.46, 19.82, 3.14, 9845.64,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008076, 90000008076, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_04', 'IOT_WIND_KL_04', 'KL01', 'Sagar Energy',
# MAGIC  34.82, 16.45, 3.48, 11420.42,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008077, 90000008077, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_04', 'IOT_WIND_KL_04', 'KL01', 'Sagar Energy',
# MAGIC  35.36, 14.28, 3.74, 12180.25,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008078, 90000008078, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_04', 'IOT_WIND_KL_04', 'KL01', 'Sagar Energy',
# MAGIC  36.48, 11.86, 4.22, 13045.73,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008079, 90000008079, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_04', 'IOT_WIND_KL_04', 'KL01', 'Sagar Energy',
# MAGIC  37.62, 8.94, 4.86, 14160.58,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008080, 90000008080, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_04', 'IOT_WIND_KL_04', 'KL01', 'Sagar Energy',
# MAGIC  38.74, 6.82, 5.62, 15480.91,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30');
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- WIND_KL_05
# MAGIC -- SequenceNumber: 80000008081 - 80000008090
# MAGIC -- ============================================================
# MAGIC
# MAGIC INSERT INTO helios.bronze.kl_wind_telemetry
# MAGIC (
# MAGIC     SequenceNumber,
# MAGIC     Offset,
# MAGIC     EnqueuedTimeUtc,
# MAGIC     asset_id,
# MAGIC     iot_id,
# MAGIC     site_id,
# MAGIC     company_name,
# MAGIC     windmill_speed,
# MAGIC     power_generated,
# MAGIC     vibration_intensity,
# MAGIC     heat_generated,
# MAGIC     event_timestamp,
# MAGIC     day_night,
# MAGIC     timestamp
# MAGIC )
# MAGIC VALUES
# MAGIC (80000008081, 90000008081, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_05', 'IOT_WIND_KL_05', 'KL01', 'Sagar Energy',
# MAGIC  11.84, 38.92, 0.86, 3540.62,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008082, 90000008082, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_05', 'IOT_WIND_KL_05', 'KL01', 'Sagar Energy',
# MAGIC  20.42, 57.84, 1.68, 5625.27,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008083, 90000008083, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_05', 'IOT_WIND_KL_05', 'KL01', 'Sagar Energy',
# MAGIC  30.18, 76.26, 2.36, 7745.48,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008084, 90000008084, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_05', 'IOT_WIND_KL_05', 'KL01', 'Sagar Energy',
# MAGIC  32.72, 22.68, 2.74, 8365.76,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008085, 90000008085, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_05', 'IOT_WIND_KL_05', 'KL01', 'Sagar Energy',
# MAGIC  33.58, 19.42, 3.16, 9650.34,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008086, 90000008086, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_05', 'IOT_WIND_KL_05', 'KL01', 'Sagar Energy',
# MAGIC  34.76, 16.84, 3.44, 11185.57,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008087, 90000008087, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_05', 'IOT_WIND_KL_05', 'KL01', 'Sagar Energy',
# MAGIC  35.48, 13.74, 3.68, 12085.83,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008088, 90000008088, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_05', 'IOT_WIND_KL_05', 'KL01', 'Sagar Energy',
# MAGIC  36.36, 11.28, 4.08, 12980.45,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008089, 90000008089, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_05', 'IOT_WIND_KL_05', 'KL01', 'Sagar Energy',
# MAGIC  37.54, 8.62, 4.72, 14125.68,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008090, 90000008090, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_05', 'IOT_WIND_KL_05', 'KL01', 'Sagar Energy',
# MAGIC  38.86, 6.74, 5.42, 15360.72,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30');
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- WIND_KL_06
# MAGIC -- SequenceNumber: 80000008091 - 80000008100
# MAGIC -- ============================================================
# MAGIC
# MAGIC INSERT INTO helios.bronze.kl_wind_telemetry
# MAGIC (
# MAGIC     SequenceNumber,
# MAGIC     Offset,
# MAGIC     EnqueuedTimeUtc,
# MAGIC     asset_id,
# MAGIC     iot_id,
# MAGIC     site_id,
# MAGIC     company_name,
# MAGIC     windmill_speed,
# MAGIC     power_generated,
# MAGIC     vibration_intensity,
# MAGIC     heat_generated,
# MAGIC     event_timestamp,
# MAGIC     day_night,
# MAGIC     timestamp
# MAGIC )
# MAGIC VALUES
# MAGIC (80000008091, 90000008091, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_06', 'IOT_WIND_KL_06', 'KL01', 'Sagar Energy',
# MAGIC  14.26, 47.38, 1.08, 4210.55,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008092, 90000008092, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_06', 'IOT_WIND_KL_06', 'KL01', 'Sagar Energy',
# MAGIC  22.64, 63.62, 1.82, 5875.82,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008093, 90000008093, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_06', 'IOT_WIND_KL_06', 'KL01', 'Sagar Energy',
# MAGIC  31.42, 80.76, 2.46, 7945.36,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008094, 90000008094, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_06', 'IOT_WIND_KL_06', 'KL01', 'Sagar Energy',
# MAGIC  32.58, 23.42, 2.78, 8520.48,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008095, 90000008095, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_06', 'IOT_WIND_KL_06', 'KL01', 'Sagar Energy',
# MAGIC  33.64, 20.18, 3.08, 9725.63,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008096, 90000008096, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_06', 'IOT_WIND_KL_06', 'KL01', 'Sagar Energy',
# MAGIC  34.72, 17.26, 3.46, 11485.27,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008097, 90000008097, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_06', 'IOT_WIND_KL_06', 'KL01', 'Sagar Energy',
# MAGIC  35.36, 14.62, 3.76, 12195.91,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008098, 90000008098, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_06', 'IOT_WIND_KL_06', 'KL01', 'Sagar Energy',
# MAGIC  36.48, 11.54, 4.18, 13160.38,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008099, 90000008099, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_06', 'IOT_WIND_KL_06', 'KL01', 'Sagar Energy',
# MAGIC  37.72, 8.86, 4.82, 14325.64,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30'),
# MAGIC
# MAGIC (80000008100, 90000008100, '2026-09-10T08:30:00Z',
# MAGIC  'WIND_KL_06', 'IOT_WIND_KL_06', 'KL01', 'Sagar Energy',
# MAGIC  38.92, 6.58, 5.58, 15640.46,
# MAGIC  '2026-09-10T14:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T14:00:00+05:30');

# COMMAND ----------

# MAGIC %md
# MAGIC # AP SOLAR INSERT STATEMENTS

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO helios.bronze.ap_solar_telemetry
# MAGIC (
# MAGIC     SequenceNumber,
# MAGIC     Offset,
# MAGIC     EnqueuedTimeUtc,
# MAGIC     line_id,
# MAGIC     iot_device_id,
# MAGIC     site_id,
# MAGIC     company_name,
# MAGIC     line_angle,
# MAGIC     number_of_panels,
# MAGIC     panel_temperature,
# MAGIC     power_generated,
# MAGIC     event_timestamp,
# MAGIC     day_night,
# MAGIC     timestamp
# MAGIC )
# MAGIC VALUES
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- LINE_AP_02
# MAGIC -- ============================================================
# MAGIC
# MAGIC (80000008051, 90000008051, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_02', 'IOT_SOL_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 28.42, 56.38,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008052, 90000008052, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_02', 'IOT_SOL_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 32.76, 53.84,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008053, 90000008053, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_02', 'IOT_SOL_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 36.24, 49.72,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008054, 90000008054, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_02', 'IOT_SOL_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 39.68, 47.35,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008055, 90000008055, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_02', 'IOT_SOL_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 43.57, 44.82,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008056, 90000008056, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_02', 'IOT_SOL_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 47.26, 41.63,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008057, 90000008057, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_02', 'IOT_SOL_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 51.84, 38.47,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008058, 90000008058, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_02', 'IOT_SOL_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 57.42, 28.64,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008059, 90000008059, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_02', 'IOT_SOL_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 63.18, 23.47,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008060, 90000008060, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_02', 'IOT_SOL_AP_02', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 68.74, 18.92,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- LINE_AP_03
# MAGIC -- ============================================================
# MAGIC
# MAGIC (80000008061, 90000008061, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_03', 'IOT_SOL_AP_03', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 24.86, 58.42,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008062, 90000008062, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_03', 'IOT_SOL_AP_03', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 30.54, 55.76,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008063, 90000008063, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_03', 'IOT_SOL_AP_03', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 35.82, 50.38,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008064, 90000008064, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_03', 'IOT_SOL_AP_03', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 38.46, 48.91,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008065, 90000008065, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_03', 'IOT_SOL_AP_03', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 42.73, 45.67,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008066, 90000008066, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_03', 'IOT_SOL_AP_03', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 46.58, 42.18,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008067, 90000008067, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_03', 'IOT_SOL_AP_03', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 53.26, 36.94,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008068, 90000008068, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_03', 'IOT_SOL_AP_03', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 58.84, 27.86,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008069, 90000008069, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_03', 'IOT_SOL_AP_03', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 64.37, 22.54,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30'),
# MAGIC
# MAGIC (80000008070, 90000008070, '2026-09-10T07:30:00Z',
# MAGIC  'LINE_AP_03', 'IOT_SOL_AP_03', 'AP01', 'Sagar Energy',
# MAGIC  45, 10, 69.42, 17.68,
# MAGIC  '2026-09-10T13:00:00+05:30', 'DAY',
# MAGIC  '2026-09-10T13:00:00+05:30');

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from helios.gold.wind_telemetry where event_timestamp = '2026-09-10T14:00:00+05:30'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from helios.gold.wind_telemetry