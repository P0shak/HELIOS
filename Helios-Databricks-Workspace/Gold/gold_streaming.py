# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

solar_stream_df = spark.readStream.table(
    "helios.silver.ap_solar_telemetry"
)

solar_stream_df = solar_stream_df.unionByName(
    spark.readStream.table("helios.silver.kl_solar_telemetry")
)

solar_stream_df = solar_stream_df.unionByName(
    spark.readStream.table("helios.silver.ks_solar_telemetry")
)

solar_stream_df = solar_stream_df.unionByName(
    spark.readStream.table("helios.silver.tn_solar_telemetry")
)

solar_stream_df = solar_stream_df.unionByName(
    spark.readStream.table("helios.silver.ts_solar_telemetry")
)

# Remove unwanted columns and add created_at
solar_stream_df = (
    solar_stream_df
    .drop("EnqueuedTimeUtc", "timestamp")
    .withColumn("created_at", current_timestamp())
)

# COMMAND ----------

(
    solar_stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option(
        "checkpointLocation",
        "/Volumes/helios/default/landing/autoloader/checkpoint/gold/solar_telemetry"
    )
    .toTable("helios.gold.solar_telemetry")
)

# COMMAND ----------

# %sql
# SELECT * FROM helios.gold.solar_telemetry

# COMMAND ----------

wind_stream_df = spark.readStream.table(
    "helios.silver.ap_wind_telemetry"
)

wind_stream_df = wind_stream_df.unionByName(
    spark.readStream.table("helios.silver.kl_wind_telemetry")
)

wind_stream_df = wind_stream_df.unionByName(
    spark.readStream.table("helios.silver.ks_wind_telemetry")
)

wind_stream_df = wind_stream_df.unionByName(
    spark.readStream.table("helios.silver.tn_wind_telemetry")
)

wind_stream_df = wind_stream_df.unionByName(
    spark.readStream.table("helios.silver.ts_wind_telemetry")
)

# Remove unwanted columns and add created_at
wind_stream_df = (
    wind_stream_df
    .drop("EnqueuedTimeUtc", "timestamp")
    .withColumn("created_at", current_timestamp())
)

# COMMAND ----------

(
    wind_stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option(
        "checkpointLocation",
        "/Volumes/helios/default/landing/autoloader/checkpoint/gold/wind_telemetry"
    )
    .toTable("helios.gold.wind_telemetry")
)

# COMMAND ----------

weather_stream_df = spark.readStream.table(
    "helios.silver.ap_weather_telemetry"
)

weather_stream_df = weather_stream_df.unionByName(
    spark.readStream.table("helios.silver.kl_weather_telemetry")
)

weather_stream_df = weather_stream_df.unionByName(
    spark.readStream.table("helios.silver.ks_weather_telemetry")
)

weather_stream_df = weather_stream_df.unionByName(
    spark.readStream.table("helios.silver.tn_weather_telemetry")
)

# COMMAND ----------

(
    weather_stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option(
        "checkpointLocation",
        "/Volumes/helios/default/landing/autoloader/checkpoint/gold/weather_telemetry"
    )
    .toTable("helios.gold.weather_telemetry")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM helios.gold.weather_telemetry

# COMMAND ----------

