# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog helios

# COMMAND ----------

# MAGIC %md
# MAGIC # AP weather

# COMMAND ----------

ap_weather = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.ap_weather_telemetry")

# COMMAND ----------

# ap_weather.printSchema()

# COMMAND ----------

# from pyspark.sql import functions as F

# ap_weather_silver = (
#     ap_weather
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# ap_weather_silver.write.mode("append").saveAsTable("helios.silver.ap_weather_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.ap_weather_telemetry"
silver_table = "helios.silver.ap_weather_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    ap_weather_new = ap_weather.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    ap_weather_new = ap_weather


# Remove duplicates and write to Silver
ap_weather_silver = (
    ap_weather_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

ap_weather_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # TS weather

# COMMAND ----------

ts_weather = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.ts_weather_telemetry")

# COMMAND ----------

# from pyspark.sql import functions as F

# ts_weather_silver = (
#     ts_weather
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# ts_weather_silver.write.mode("append").saveAsTable("helios.silver.ts_weather_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.ts_weather_telemetry"
silver_table = "helios.silver.ts_weather_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    ts_weather_new = ts_weather.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    ts_weather_new = ts_weather


# Remove duplicates and write to Silver
ts_weather_silver = (
    ts_weather_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

ts_weather_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # TN weather

# COMMAND ----------

tn_weather = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.tn_weather_telemetry")

# COMMAND ----------

# from pyspark.sql import functions as F

# tn_weather_silver = (
#     tn_weather
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# tn_weather_silver.write.mode("append").saveAsTable("helios.silver.tn_weather_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.tn_weather_telemetry"
silver_table = "helios.silver.tn_weather_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    tn_weather_new = tn_weather.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    tn_weather_new = tn_weather


# Remove duplicates and write to Silver
tn_weather_silver = (
    tn_weather_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

tn_weather_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # KL weather

# COMMAND ----------

kl_weather = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.kl_weather_telemetry")

# COMMAND ----------

# from pyspark.sql import functions as F

# kl_weather_silver = (
#     kl_weather
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# kl_weather_silver.write.mode("append").saveAsTable("helios.silver.kl_weather_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.kl_weather_telemetry"
silver_table = "helios.silver.kl_weather_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    kl_weather_new = kl_weather.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    kl_weather_new = kl_weather


# Remove duplicates and write to Silver
kl_weather_silver = (
    kl_weather_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

kl_weather_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # KS weather

# COMMAND ----------

ks_weather = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.ks_weather_telemetry")

# COMMAND ----------

# from pyspark.sql import functions as F

# ks_weather_silver = (
#     ks_weather
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# ks_weather_silver.write.mode("append").saveAsTable("helios.silver.ks_weather_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.ks_weather_telemetry"
silver_table = "helios.silver.ks_weather_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    ks_weather_new = ks_weather.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    ks_weather_new = ks_weather


# Remove duplicates and write to Silver
ks_weather_silver = (
    ks_weather_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

ks_weather_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)