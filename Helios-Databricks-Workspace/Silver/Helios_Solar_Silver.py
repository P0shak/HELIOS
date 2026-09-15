# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog helios

# COMMAND ----------

# MAGIC %md
# MAGIC # AP Solar

# COMMAND ----------

ap_solar = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.ap_solar_telemetry")

# COMMAND ----------

# from pyspark.sql import functions as F

# ap_solar_silver = (
#     ap_solar
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# ap_solar_silver.write.mode("append").saveAsTable("helios.silver.ap_solar_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.ap_solar_telemetry"
silver_table = "helios.silver.ap_solar_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    ap_solar_new = ap_solar.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    ap_solar_new = ap_solar


# Remove duplicates and write to Silver
ap_solar_silver = (
    ap_solar_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

ap_solar_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # TS Solar

# COMMAND ----------

ts_solar = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.ts_solar_telemetry")

# COMMAND ----------

# from pyspark.sql import functions as F

# ap_solar_silver = (
#     ts_solar
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# ap_solar_silver.write.mode("append").saveAsTable("helios.silver.ts_solar_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.ts_solar_telemetry"
silver_table = "helios.silver.ts_solar_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    ts_solar_new = ts_solar.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    ts_solar_new = ts_solar

# give if condition count > 0 
# Remove duplicates and write to Silver
ts_solar_silver = (
    ts_solar_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

ts_solar_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # TN Solar

# COMMAND ----------

tn_solar = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.tn_solar_telemetry")

# COMMAND ----------

# from pyspark.sql import functions as F

# tn_solar_silver = (
#     tn_solar
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# tn_solar_silver.write.mode("append").saveAsTable("helios.silver.tn_solar_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.tn_solar_telemetry"
silver_table = "helios.silver.tn_solar_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    tn_solar_new = tn_solar.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    tn_solar_new = tn_solar


# Remove duplicates and write to Silver
tn_solar_silver = (
    tn_solar_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

tn_solar_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # KL Solar

# COMMAND ----------

kl_solar = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.kl_solar_telemetry")

# COMMAND ----------

# from pyspark.sql import functions as F

# kl_solar_silver = (
#     kl_solar
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# kl_solar_silver.write.mode("append").saveAsTable("helios.silver.kl_solar_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.kl_solar_telemetry"
silver_table = "helios.silver.kl_solar_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    kl_solar_new = kl_solar.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    kl_solar_new = kl_solar


# Remove duplicates and write to Silver
kl_solar_silver = (
    kl_solar_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

kl_solar_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # KS Solar

# COMMAND ----------

ks_solar = spark.sql("select * EXCEPT(Offset, SequenceNumber,timestamp), Offset as gen_id from helios.bronze.ks_solar_telemetry")

# COMMAND ----------

# from pyspark.sql import functions as F

# ks_solar_silver = (
#     ks_solar
#     .dropDuplicates(["gen_id", "event_timestamp"])
#     .withColumn("timestamp", F.current_timestamp())
# )

# ks_solar_silver.write.mode("append").saveAsTable("helios.silver.ks_solar_telemetry")

# COMMAND ----------

from pyspark.sql import functions as F

bronze_table = "helios.bronze.ks_solar_telemetry"
silver_table = "helios.silver.ks_solar_telemetry"

# Get the last processed timestamp from Silver
last_timestamp = (
    spark.table(silver_table)
    .agg(F.max("timestamp").alias("last_timestamp"))
    .collect()[0]["last_timestamp"]
)

print("Last processed timestamp:", last_timestamp)


# Process only new records
if last_timestamp is not None:

    ks_solar_new = ks_solar.filter(
        F.col("timestamp") > F.lit(last_timestamp)
    )

else:
    # First run - process everything
    ks_solar_new = ks_solar


# Remove duplicates and write to Silver
ks_solar_silver = (
    ks_solar_new
    .dropDuplicates(["gen_id", "event_timestamp"])
)

ks_solar_silver.write \
    .mode("append") \
    .saveAsTable(silver_table)