# Databricks notebook source
jdbc_url = (
    "jdbc:sqlserver://priya-database.database.windows.net:1433;"
    "databaseName=priya-db;"
    "encrypt=true;"
    "trustServerCertificate=false;"
)

properties = {
    "user": "demosql",
    "password": "demo@9676",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

sites_df = spark.read.jdbc(
    url=jdbc_url,
    table="helios_assets.sites",
    properties=properties
)

# display(sites_df)

# COMMAND ----------

solar_df = spark.read.jdbc(
    url=jdbc_url,
    table="helios_assets.solar_assets",
    properties=properties
)
# display(solar_df)

# COMMAND ----------

wind_df = spark.read.jdbc(
    url=jdbc_url,
    table="helios_assets.wind_assets",
    properties=properties
)
# display(wind_df)

# COMMAND ----------

iot_devices_df = spark.read.jdbc(
    url=jdbc_url,
    table="helios_assets.iot_devices",
    properties=properties
)
# display(iot_devices_df)

# COMMAND ----------

# sites_df.printSchema()

# solar_df.printSchema()

# wind_df.printSchema()

# iot_devices_df.printSchema()

# COMMAND ----------

(
    sites_df.write
    .format("delta")
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable("helios.gold.sites")
)

# COMMAND ----------

(
    solar_df.write
    .format("delta")
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable("helios.gold.solar_assets")
)


# COMMAND ----------

(
    wind_df.write
    .format("delta")
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable("helios.gold.wind_assets")
)

# COMMAND ----------

(
    iot_devices_df.write
    .format("delta")
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable("helios.gold.iot_devices")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM helios.gold.sites