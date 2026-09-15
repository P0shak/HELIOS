# Databricks notebook source
SOURCE_PATH = "abfss://helios-raw-storage@projectstorage12.dfs.core.windows.net/solarwind-eventhub-ns/helios_wind"

SCHEMA_PATH = "/Volumes/helios/default/landing/autoloader/schema/"

CHECKPOINT_PATH = "/Volumes/helios/default/landing/autoloader/checkpoint/bronze/wind/"

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
        asset_id STRING,
        iot_id STRING,
        site_id STRING,
        company_name STRING,
        windmill_speed DOUBLE,
        power_generated DOUBLE,
        vibration_intensity DOUBLE,
        heat_generated DOUBLE,
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
    "AP01": "ap_wind_telemetry",
    "KS01": "ks_wind_telemetry",
    "TS01": "ts_wind_telemetry",
    "TN01": "tn_wind_telemetry",
    "KL01": "kl_wind_telemetry"
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
# FROM helios.bronze.ap_wind_telemetry;

# COMMAND ----------

# %sql
# SELECT *
# FROM helios.bronze.ks_wind_telemetry
# ORDER BY event_timestamp
# -- LI;

# COMMAND ----------

# %sql
# SELECT Offset, COUNT(*) AS row_count
# FROM helios.bronze.ks_wind_telemetry
# GROUP BY Offset
# HAVING count(*) > 1