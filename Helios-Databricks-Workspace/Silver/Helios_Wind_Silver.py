# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog helios

# COMMAND ----------

# MAGIC %md
# MAGIC # AP Wind

# COMMAND ----------

ap_wind = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.ap_wind_telemetry")

# COMMAND ----------

# from pyspark.sql import functions as F

# ap_wind_silver = (
#     ap_wind
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# ap_wind_silver.write.mode("append").saveAsTable("helios.silver.ap_wind_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.ap_wind_telemetry"
silver_table = "helios.silver.ap_wind_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    ap_wind_new = ap_wind.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    ap_wind_new = ap_wind


# Remove duplicates and write to Silver
ap_wind_silver = (
    ap_wind_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

ap_wind_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # TS wind

# COMMAND ----------

ts_wind = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.ts_wind_telemetry")

# COMMAND ----------

# from pyspark.sql import functions as F

# ts_wind_silver = (
#     ts_wind
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# ts_wind_silver.write.mode("append").saveAsTable("helios.silver.ts_wind_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.ts_wind_telemetry"
silver_table = "helios.silver.ts_wind_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    ts_wind_new = ts_wind.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    ts_wind_new = ts_wind


# Remove duplicates and write to Silver
ts_wind_silver = (
    ts_wind_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

ts_wind_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # TN wind

# COMMAND ----------

tn_wind = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.tn_wind_telemetry")

# COMMAND ----------

# from pyspark.sql import functions as F

# tn_wind_silver = (
#     tn_wind
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# tn_wind_silver.write.mode("append").saveAsTable("helios.silver.tn_wind_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.tn_wind_telemetry"
silver_table = "helios.silver.tn_wind_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    tn_wind_new = tn_wind.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    tn_wind_new = tn_wind


# Remove duplicates and write to Silver
tn_wind_silver = (
    tn_wind_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

tn_wind_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # KL wind

# COMMAND ----------

kl_wind = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.kl_wind_telemetry")

# COMMAND ----------

# from pyspark.sql import functions as F

# kl_wind_silver = (
#     kl_wind
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# kl_wind_silver.write.mode("append").saveAsTable("helios.silver.kl_wind_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.kl_wind_telemetry"
silver_table = "helios.silver.kl_wind_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    kl_wind_new = kl_wind.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    kl_wind_new = kl_wind


# Remove duplicates and write to Silver
kl_wind_silver = (
    kl_wind_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

kl_wind_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # KS wind

# COMMAND ----------

ks_wind = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.ks_wind_telemetry")

# COMMAND ----------

# from pyspark.sql import functions as F

# ks_wind_silver = (
#     ks_wind
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# ks_wind_silver.write.mode("append").saveAsTable("helios.silver.ks_wind_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.ks_wind_telemetry"
silver_table = "helios.silver.ks_wind_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    ks_wind_new = ks_wind.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    ks_wind_new = ks_wind


# Remove duplicates and write to Silver
ks_wind_silver = (
    ks_wind_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

ks_wind_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)