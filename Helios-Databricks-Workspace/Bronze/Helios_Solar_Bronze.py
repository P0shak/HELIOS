# Databricks notebook source
SOURCE_PATH = "abfss://helios-raw-storage@projectstorage12.dfs.core.windows.net/solarwind-eventhub-ns/helios_solar"

SCHEMA_PATH = "/Volumes/helios/default/landing/autoloader/schema/"

CHECKPOINT_PATH = "/Volumes/helios/default/landing/autoloader/checkpoint/bronze/solar/"

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "avro")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .load(SOURCE_PATH)
)

# COMMAND ----------

from pyspark.sql import functions as F

decoded_stream = raw_stream.select(
    "SequenceNumber",
    "Offset",
    "EnqueuedTimeUtc",
    F.col("Body").cast("string").alias("json")
)

# COMMAND ----------

telemetry_stream = decoded_stream.select(
    "*",
    F.from_json(
        "json",
        """
        line_id STRING,
        iot_device_id STRING,
        site_id STRING,
        company_name STRING,
        line_angle DOUBLE,
        number_of_panels INT,
        panel_temperature DOUBLE,
        power_generated DOUBLE,
        event_timestamp TIMESTAMP,
        day_night STRING
        """
    ).alias("data")
)

# COMMAND ----------

bronze_stream = telemetry_stream.select(
    "SequenceNumber",
    "Offset",
    "EnqueuedTimeUtc",
    "data.*"
)

# COMMAND ----------

sites = {
    "AP01": "ap_solar_telemetry",
    "KL01": "kl_solar_telemetry",
    "KS01": "ks_solar_telemetry",
    "TN01": "tn_solar_telemetry",
    "TS01": "ts_solar_telemetry"
}

for site_id, table_name in sites.items():

    site_stream = (
        bronze_stream
        .filter(F.col("site_id") == site_id)
        .withColumn("timestamp", F.current_timestamp())
    )

    (
        site_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            CHECKPOINT_PATH + f"{table_name}/"
        )
        .trigger(availableNow=True)
        .toTable(
            f"helios.bronze.{table_name}"
        )
    )
    

# COMMAND ----------

# %sql
# SELECT COUNT(*) AS row_count
# FROM helios.bronze.ap_solar_telemetry;

# COMMAND ----------

# %sql
# SELECT *
# FROM helios.bronze.ks_solar_telemetry
# -- LI;

# COMMAND ----------

# %sql
# SELECT Offset, COUNT(*) AS row_count
# FROM helios.bronze.ks_solar_telemetry
# GROUP BY Offset
# HAVING count(*) > 1